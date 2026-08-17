package my.example.prac1;

public class EntityFactory {

    public static BaseItem createEntity(EntityType type, int id, String name) {
        switch (type) {
            case BOOK:
                return new Book(id, name);
            case CLIENT:
                return new Client(id, name);
            default:
                return null;
        }
    }
}
