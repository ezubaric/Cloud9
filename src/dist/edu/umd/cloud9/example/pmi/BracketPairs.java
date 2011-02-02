package edu.umd.cloud9.example.pmi;

import java.util.Iterator;
import edu.umd.cloud9.io.PairOfStrings;

public class BracketPairs implements Iterator<PairOfStrings> {
  private static BracketResults itr1 = new BracketResults("");
  private static BracketResults itr2 = new BracketResults("");

  private static int first_index;
  private static int second_index;

  private static String first;
  private static String EMPTY = "";

  PairOfStrings result = new PairOfStrings();

  public void resetString(String source) {
      itr1.resetString(source);
      itr2.resetString(source);
      if (itr1.hasNext()) {
	first = null;
      }
      itr2.skip(source.length());
  }

  public BracketPairs() {
    resetString("");
  }


  @Override
  public boolean hasNext() {
    return itr1.hasNext() || itr2.hasNext();
  }

  @Override
  public PairOfStrings next() {

    if (itr2.hasNext()) {
      String second = itr2.next();
      if (first.compareTo(second) < 0) {
	result.set(first, second);
      } else {
	result.set(second, first);
      }
    } else {
      first = itr1.next();
      result.set(EMPTY, first);

      itr2.skip(itr1.position());
    }

    return result;
  }

  @Override
  public void remove() {}

  public static void main(String [] args) {

    BracketPairs np_blocks = new BracketPairs();
    np_blocks.resetString("< George Bush > and < Bill Clinton > went to the < Kremlin >");
    while(np_blocks.hasNext()) System.out.println(np_blocks.next());
    System.out.println("--------------------------");

    np_blocks.resetString("< George Bush > and < Bill Clinton > talked");
    while(np_blocks.hasNext()) System.out.println(np_blocks.next());
    System.out.println("--------------------------");

    np_blocks.resetString("< George Bush >");
    while(np_blocks.hasNext()) System.out.println(np_blocks.next());
    System.out.println("--------------------------");

  }

}
