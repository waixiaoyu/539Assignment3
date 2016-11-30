package com.yyy.mr;

import java.io.FileInputStream;
import java.io.IOException;

import chesspresso.game.Game;
import chesspresso.pgn.PGNReader;
import chesspresso.pgn.PGNSyntaxError;

public class ReadPGN {
	private static String filename = "D:\\ficsgamesdb_small_201503.pgn";

	public static void main(String[] args) throws IOException, PGNSyntaxError {

		PGNReader pgnReader = new PGNReader(new FileInputStream(filename), filename);
		Game game = pgnReader.parseGame();
		System.out.println(game.getBlack());
		game = pgnReader.parseGame();
		System.out.println(game.getBlack());

	}
}
