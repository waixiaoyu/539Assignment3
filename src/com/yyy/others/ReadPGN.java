package com.yyy.others;

import java.io.FileInputStream;
import java.io.IOException;

import chesspresso.game.Game;
import chesspresso.pgn.PGNReader;
import chesspresso.pgn.PGNSyntaxError;

public class ReadPGN {
	private static String filename = "D:\\ficsgamesdb_small_201503.pgn";

	public static void main(String[] args) throws IOException, PGNSyntaxError {

		PGNReader pgnReader = new PGNReader(new FileInputStream(filename), filename);
		Game game;

		while (true) {
			game = pgnReader.parseGame();
			if (game == null) {
				break;
			}
			// 0->white,1->draw,2->black
			System.out.println(game.getBlack());
			System.out.println(game.getBlackElo());
			System.out.println(game.getBlackEloStr());

		}

	}
}
