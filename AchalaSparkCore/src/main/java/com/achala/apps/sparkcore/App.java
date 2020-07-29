package com.achala.apps.sparkcore;

public class App {

	public static void main(String[] args) {
		new Demo().map((a, b) -> {
			return a + b;
		});
	}

}
