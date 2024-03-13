use super::hooks::free_boxed_hook;
use super::hooks::Action;

use std::fmt::Debug;
use std::os::raw::{c_char, c_int, c_void};
use std::panic::catch_unwind;
use std::ptr;

use crate::ffi;
use crate::types::ValueRef;
use crate::{Connection, InnerConnection};

/// `feature = "preupdate_hook"`
/// The possible cases for when a PreUpdateHook gets triggered. Allows access to the relevant
/// functions for each case through the contained values.
#[derive(Debug)]
pub enum PreUpdateCase {
    Insert(PreUpdateNewValueAccessor),
    Delete(PreUpdateOldValueAccessor),
    Update {
        old_value_accessor: PreUpdateOldValueAccessor,
        new_value_accessor: PreUpdateNewValueAccessor,
    },
}

impl From<PreUpdateCase> for Action {
    fn from(puc: PreUpdateCase) -> Action {
        match puc {
            PreUpdateCase::Insert(_) => Action::SQLITE_INSERT,
            PreUpdateCase::Delete(_) => Action::SQLITE_DELETE,
            PreUpdateCase::Update { .. } => Action::SQLITE_UPDATE,
        }
    }
}

/// `feature = "preupdate_hook"`
/// An accessor to access the old values of the row being deleted/updated during the preupdate callback.
#[derive(Debug)]
pub struct PreUpdateOldValueAccessor {
    db: *mut ffi::sqlite3,
    old_row_id: i64,
}

impl PreUpdateOldValueAccessor {
    /// Get the amount of columns in the row being
    /// deleted/updated.
    pub fn get_column_count(&self) -> i32 {
        unsafe { ffi::sqlite3_preupdate_count(self.db) }
    }

    pub fn get_query_depth(&self) -> i32 {
        unsafe { ffi::sqlite3_preupdate_depth(self.db) }
    }

    pub fn get_old_row_id(&self) -> i64 {
        self.old_row_id
    }

    pub fn get_old_column_value(&self, i: i32) -> ValueRef {
        let mut p_value: *mut ffi::sqlite3_value = ptr::null_mut();
        unsafe {
            ffi::sqlite3_preupdate_old(self.db, i, &mut p_value);
            ValueRef::from_value(p_value)
        }
    }
}

/// `feature = "preupdate_hook"`
/// An accessor to access the new values of the row being inserted/updated during the preupdate callback.
#[derive(Debug)]
pub struct PreUpdateNewValueAccessor {
    db: *mut ffi::sqlite3,
    new_row_id: i64,
}

impl PreUpdateNewValueAccessor {
    /// Get the amount of columns in the row being
    /// inserted/updated.
    pub fn get_column_count(&self) -> i32 {
        unsafe { ffi::sqlite3_preupdate_count(self.db) }
    }

    pub fn get_query_depth(&self) -> i32 {
        unsafe { ffi::sqlite3_preupdate_depth(self.db) }
    }

    pub fn get_new_row_id(&self) -> i64 {
        self.new_row_id
    }

    pub fn get_new_column_value(&self, i: i32) -> ValueRef {
        let mut p_value: *mut ffi::sqlite3_value = ptr::null_mut();
        unsafe {
            ffi::sqlite3_preupdate_new(self.db, i, &mut p_value);
            ValueRef::from_value(p_value)
        }
    }
}

impl Connection {
    ///
    /// `feature = "preupdate_hook"` Register a callback function to be invoked before
    /// a row is updated, inserted or deleted in a rowid table.
    ///
    /// The callback parameters are:
    ///
    /// - the name of the database ("main", "temp", ...),
    /// - the name of the table that is updated,
    /// - a variant of the PreUpdateCase enum which allows access to extra functions depending
    /// on whether it's an update, delete or insert.
    #[inline]
    pub fn preupdate_hook<'c, F>(&'c self, hook: Option<F>)
    where
        F: FnMut(Action, &str, &str, &PreUpdateCase) + Send + 'c,
    {
        self.db.borrow_mut().preupdate_hook(hook);
    }
}

impl InnerConnection {
    #[inline]
    pub fn remove_preupdate_hook(&mut self) {
        self.preupdate_hook(None::<fn(Action, &str, &str, &PreUpdateCase)>);
    }

    fn preupdate_hook<'c, F>(&'c mut self, hook: Option<F>)
    where
        F: FnMut(Action, &str, &str, &PreUpdateCase) + Send + 'c,
    {
        unsafe extern "C" fn call_boxed_closure<F>(
            p_arg: *mut c_void,
            sqlite: *mut ffi::sqlite3,
            action_code: c_int,
            db_str: *const c_char,
            tbl_str: *const c_char,
            old_row_id: i64,
            new_row_id: i64,
        ) where
            F: FnMut(Action, &str, &str, &PreUpdateCase),
        {
            use std::ffi::CStr;
            use std::str;

            let action = Action::from(action_code);
            let db_name = {
                let c_slice = CStr::from_ptr(db_str).to_bytes();
                str::from_utf8(c_slice)
            };
            let tbl_name = {
                let c_slice = CStr::from_ptr(tbl_str).to_bytes();
                str::from_utf8(c_slice)
            };

            let preupdate_hook_functions = match action {
                Action::SQLITE_INSERT => PreUpdateCase::Insert(PreUpdateNewValueAccessor {
                    db: sqlite,
                    new_row_id,
                }),
                Action::SQLITE_DELETE => PreUpdateCase::Delete(PreUpdateOldValueAccessor {
                    db: sqlite,
                    old_row_id,
                }),
                Action::SQLITE_UPDATE => PreUpdateCase::Update {
                    old_value_accessor: PreUpdateOldValueAccessor {
                        db: sqlite,
                        old_row_id,
                    },
                    new_value_accessor: PreUpdateNewValueAccessor {
                        db: sqlite,
                        new_row_id,
                    },
                },
                _ => todo!(),
            };

            let _ = catch_unwind(|| {
                let boxed_hook: *mut F = p_arg as *mut F;
                (*boxed_hook)(
                    action,
                    db_name.expect("illegal db name"),
                    tbl_name.expect("illegal table name"),
                    &preupdate_hook_functions,
                );
            });
        }

        let free_preupdate_hook = if hook.is_some() {
            Some(free_boxed_hook::<F> as unsafe fn(*mut c_void))
        } else {
            None
        };

        let previous_hook = match hook {
            Some(hook) => {
                let boxed_hook: *mut F = Box::into_raw(Box::new(hook));
                unsafe {
                    ffi::sqlite3_preupdate_hook(
                        self.db(),
                        Some(call_boxed_closure::<F>),
                        boxed_hook as *mut _,
                    )
                }
            }
            _ => unsafe { ffi::sqlite3_preupdate_hook(self.db(), None, ptr::null_mut()) },
        };
        if !previous_hook.is_null() {
            if let Some(free_boxed_hook) = self.free_preupdate_hook {
                unsafe { free_boxed_hook(previous_hook) };
            }
        }
        self.free_preupdate_hook = free_preupdate_hook;
    }
}
