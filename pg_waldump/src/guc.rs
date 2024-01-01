// The possible values of an enum variable are specified by an array of
// name-value pairs.  The "hidden" flag means the value is accepted but
// won't be displayed when guc.c is asked for a list of acceptable values.
pub struct ConfigEnumEntry {
    pub name: &'static str,
    pub val: i32,
    pub hidden: bool,
}
