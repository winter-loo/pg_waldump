use crate::pgtypes::*;

pub fn array_desc<T, F>(array: &[T], elem_desc: F) -> String 
    where F: Fn(&T) -> String
{
    let mut buf = String::new();
    if array.len() == 0 {
        buf.push_str(" []");
        return buf;
    }
    buf.push_str(" [");
    for (i, e) in array.iter().enumerate() {
        buf += &elem_desc(&e);
        if i < array.len() - 1 {
            buf.push_str(", ");
        }
    }
    buf.push(']');
    buf
}

pub fn oid_elem_desc(relid: &Oid) -> String
{
	format!("{}", relid)
}

pub fn offset_elem_desc(offset: &OffsetNumber) -> String
{
	format!("{}", offset)
}
