package hfile

/*
Attempting to make sense of the vint impl from hadoop:
http://grepcode.com/file/repo1.maven.org/maven2/org.apache.hadoop/hadoop-common/2.5.0/org/apache/hadoop/io/WritableUtils.java#WritableUtils.readVInt%28java.io.DataInput%29

Mapping the range of possible values of the first byte byte (in java where they are for some reason signed):
              neg*       pos*             neg                pos
java     |-128     | -120       | -112           -1  | 0               127 |
hex      |0x80     | 0x88       | 0x90           0xff| 0x00            0x7F|

Regions without the * are just used as-is: that is the value of the vint, and the vint is 1 byte in lenght.
Regions with the * each contain possible 8 values, indicating how many *more* bytes make up this vint.
After those values are read, if the count was from the "neg" region, the result is OR'ed with -1.

Ordering those regions without java's silly signing:
                  pos                 neg*      pos*           neg
hex      | 0x00            0x7F | 0x80     | 0x88       | 0x90      0xff |
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
