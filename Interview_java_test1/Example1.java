import java.util.ArrayList;
import java.util.List;

public interface IWeighable {

    public double getWeight();
}

public class PhysicalObject implements IWeighable {

    private double weight;

    public PhysicalObject(double w) {

        this.weight = w;
    }
    public double getWeight() {

        return this.weight;
    }
}

public class Book extends PhysicalObject {

    public Book(double w) {

        super(w);
    }
}

public class ComicBook extends Book {

    public ComicBook(double w) {

        super(w);
    }
}
public class Laptop extends PhysicalObject implements IWeighable {

    public Laptop(double w) {

        super(w);
    }
}

public class ShippingBox<T extends IWeighable> {

    private List<T> items;

    public ShippingBox() {

        this.items = new ArrayList<>();
    }
    
    public void addItem(T item) {

        this.items.add(item);
    }

    public List<T> getItems() {
        return this.items;
    }

    public double getTotalWeight() {

        double totalW = 0;
        for (T item: this.items) {

            totalW += item.getWeight();
        }

        return totalW;
    }

    public void addAnyWeighable(ShippingBox<? extends T> shippingBox) {

        for (T item: shippingBox.getItems()) {

            this.items.add(item);
        }
    }
}


public class Example1 {

    public void test() {
        Book book1 = new Book(1.2);
        Book book2 = new Book(1.2);
        Laptop laptop = new Laptop(2.2);
        ComicBook comicBook = new ComicBook(41);

        ShippingBox<PhysicalObject> bigBox = new ShippingBox<>();
        bigBox.addItem(book1);
        bigBox.addItem(book2);
        bigBox.addItem(laptop);
        bigBox.addItem(comicBook);

        List<Class<? extends PhysicalObject>> objects = new ArrayList<>();

        objects.add(ComicBook.class);
        objects.add(Laptop.class);

        for (Class<? extends PhysicalObject> obj : objects) {

            System.out.printf("obj class: ", obj.getName());
        }

    }
}