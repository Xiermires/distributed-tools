package org.distributed.dht;

import java.math.BigInteger;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

public class Keys {

    @SuppressWarnings("deprecation")
    public static BigInteger of(String s) {
	return new BigInteger(Hashing.sha1().hashString(s, Charsets.UTF_8).toString(), 16);
    }
}
