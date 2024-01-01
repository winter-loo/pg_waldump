# pg_waldump

## Features

* dump supported resources:

    * xlog
    * database
    * heap
    * btree


## developement

### check dump result

**strategy**: comparing results from official pg_waldump and current pg_waldump

For official pg_waldump command, execute as,

```shell
pg_waldump 000000010000000000000001 -r xlog -b > foo.expt
```

For current pg_waldump,

```
cargo run -- 000000010000000000000001 -r xlog > foo.rst
```

### pointer arithmetic

C code,

```C
for (int p = 0; p < updates->ndeletedtids; p++)
{
	uint16	   *ptid;

	ptid = (uint16 *) ((char *) updates + SizeOfBtreeUpdate) + p;
	appendStringInfo(buf, "%u", *ptid);

	if (p < updates->ndeletedtids - 1)
		appendStringInfoString(buf, ", ");
}
```

rust code,

```rust
for p in 0..updates.ndeletedtids {
    let ptid = unsafe {
        *((updates as *const XLogBtreeUpdate as *const u8)
            .add(SIZE_OF_BTREE_UPDATE) as *const u16).add(p as usize)
    };
    buf += &format!("{}", ptid);

    if (p < updates.ndeletedtids - 1) {
        buf.push_str(", ");
    }
}
```

