package my.example.prac1;

public class Book extends BaseItem {

    private final int numPages;
    private final String bookAuthor;

    public Book(int id, String name) {
        super(id, name);
        this.numPages = 0;
        this.bookAuthor = "Unknown";
    }

    public Book(int id, String name, int numPages, String bookAuthor) {
        super(id, name);
        this.numPages = numPages;
        this.bookAuthor = bookAuthor;
    }

    public int getNumPages() {
        return numPages;
    }

    public String getBookAuthor() {
        return bookAuthor;
    }

    @Override
    public String toString() {
        return "Book{id=" + super.getId() + ", name='" + super.getName() + "', numPages=" + numPages + ", bookAuthor='" + bookAuthor + "' }";
    }
}
