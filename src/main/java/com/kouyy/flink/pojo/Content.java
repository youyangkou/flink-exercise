package com.kouyy.flink.pojo;

public class Content {

        private BookOrder bookOrder;


        public void setBookOrder(BookOrder bookOrder) {
            this.bookOrder = bookOrder;
        }
        public BookOrder getBookOrder() {
            return bookOrder;
        }

    @Override
    public String toString() {
        return "Content{" +
                "bookOrder=" + bookOrder +
                '}';
    }
}
