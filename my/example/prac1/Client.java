package my.example.prac1;

public class Client extends BaseItem {

    final int age;
    final String email;

    public Client(int id, String name) {
        super(id, name);
        this.age = 0;
        this.email = "Unknown";
    }

    public Client(int id, String name, int age, String email) {
        super(id, name);
        this.age = age;
        this.email = email;
    }

    public int getAge() {
        return age;
    }

    public String getEmail() {
        return email;
    }

    @Override
    public String toString() {
        return "Client{id=" + super.getId() + ", name='" + super.getName() + "', age=" + age + ", email='" + email + "'}";
    }
}
