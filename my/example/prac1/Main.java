package my.example.prac1;

public class Main {
    
    public static void main(String[] args) {
        BaseItem book = EntityFactory.createEntity(EntityType.BOOK, 40, "1984");
        BaseItem client = EntityFactory.createEntity(EntityType.CLIENT, 10, "Alice");

        System.out.println(book);
        System.out.println(client);
    }
}
