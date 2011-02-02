package edu.umd.cloud9.example.pmi;

import java.util.Iterator;

public class BracketResults implements Iterator<String> {
  private String data;
  private int index;
  private int length;

  public void resetString(String source) {
    this.data = source;
    this.index = source.indexOf('<') + 2;
    this.length = this.data.length();    
  }

  public BracketResults(String source) {
      resetString(source);
  }

  @Override
  public boolean hasNext() {
    return this.index < this.length;
  }

  @Override
  public String next() {
    int end = this.data.indexOf('>', this.index) - 1;
    if (end < this.index) {
	end = this.length;
	System.out.println("~" + this.data.substring(this.index, end) + "~");
    }
    String result = this.data.substring(this.index, end);

    this.index = this.data.indexOf('<', end);
    if (this.index < 0) this.index = this.length;
    else this.index += 2;
    return result;
  }

  public int position() {
      return this.index;
  }

  public void skip(int index) {
      this.index = index;
  }

  @Override
  public void remove() {}

  public static void main(String [] args) {

    BracketResults np_blocks = new BracketResults("");
    np_blocks.resetString("< George Bush > and < Bill Clinton > went to the < Kremlin >");
    while(np_blocks.hasNext()) {
      System.out.println("~" + np_blocks.next() + "~");
    }
  }

}
