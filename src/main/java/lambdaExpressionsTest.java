import java.util.ArrayList;

class Person {
    String name;
    int age;
    boolean gender;
    public Person(String name, int age, boolean gender) {
        this.name=name;
        this.age=age;
        this.gender=gender;
    }
    public String toString() {
        return name + "," + age + "," + gender;
    }
    public void print() {
        System.out.println(this);
    }
}

interface pfilter {
    boolean test(Person p);
}
class theAdults implements pfilter {
    public boolean test(Person p) {
        return p.age>=18;
    }
}

public class lambdaExpressionsTest {

    public static void printAllWithFilter(ArrayList<Person> persons, pfilter f) {
        System.out.println("Filtering results ");
        for (Person p:persons)
            if (f.test(p)) p.print();
    }

    public static void main(String[] args) {
        ArrayList<Person> persons = new ArrayList<>();
        persons.add(new Person("John", 10, true));
        persons.add(new Person("Jill", 15, false));
        persons.add(new Person("James", 20, true));

        theAdults adultsFilter = new theAdults();
        printAllWithFilter(persons, adultsFilter);

        printAllWithFilter(persons, new pfilter() {
            public boolean test(Person p) {
                return p.age>=18;
            }
        });

        printAllWithFilter(persons, (Person p) -> (p.age>=18));
        persons.forEach((Person p) -> System.out.println(p));

        int count=0;
        persons.forEach((Person p) -> {
//            count++;
            if (p.gender==true)
                System.out.println(p.name + " is a male aged " + p.age + "." + count);
            else
                System.out.println(p.name + " is a fmale aged " + p.age + ".");
        });


    }

}

