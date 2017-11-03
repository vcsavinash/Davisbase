package com.axc166930.davisbase;

import java.io.FileNotFoundException;

/**
 * @author avinash
 *
 */
public class BPlusTree {
	RandomAccessFileCreation root;
	
	/**
	 * @param f
	 * @throws FileNotFoundException
	 */
	public BPlusTree(RandomAccessFileCreation f) throws FileNotFoundException {
		root = f;
	}
	
}

