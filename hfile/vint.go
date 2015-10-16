package hfile

/*
Implementation of hadoop's variable-length, signed int:
http://grepcode.com/file/repo1.maven.org/maven2/org.apache.hadoop/hadoop-common/2.5.0/org/apache/hadoop/io/WritableUtils.java#WritableUtils.readVInt%28java.io.DataInput%29

As far as I understand, the above-mentioned java implementation works as follows:

A first byte in range 0 to 0x7f, or 0x90 to 0xff (ie without a * below):
- the vint value is just the first byte, and the length is just 1.
- NB: values between 0x90 and 0xff are negative thanks to java's signed byte type.

A first byte with a value between 0x80 and 0x8F (neg* and pos* below):
 - first byte indicates the length, and sign, of the vint.
 - there are two ranges (0x80-0x87 and 0x88-0x8f), each with 8 possible values.
 - logic same for both, except one indicates final value is negative.
 - since java was treating these as signed, appears to count "down" in go.

To represent possible first-byte values visually (in java's signed order first):
java     | -128    | -120    | -112     -1  | 0             127 |
hex      | 0x80    | 0x88    | 0x90     0xff| 0x00          0x7F|


Putting that in an unsigned (sane) order:
hex      | 0x00          0x7F| 0x80    | 0x88    | 0x90     0xff|
java     | 0             127 | -128    | -120    | -112     -1  |
meaning: | pos               | neg*    | pos*    | neg          |
*/
func vintAndLen(b []byte) (int, int) {
	first := b[0]
	count := 1
	neg := false

	if first < 0x80 {
		return int(first), count
	}

	if first >= 0x90 {
		return -256 + int(first), count
	}

	if first < 0x88 {
		neg = true
		count = int(0x88-first) + 1
	} else {
		count = int(0x90-first) + 1
	}

	ret := 0
	for i := 1; i < count; i++ {
		ret = (ret << 8) | int(b[i])
	}
	if neg {
		ret = (ret ^ -1)
	}
	return ret, count
}
