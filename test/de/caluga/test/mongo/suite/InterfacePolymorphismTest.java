package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: bfsmith
 * Date: 30.03.15
 * Test interface polymorphism mechanism in Morphium
 */
public class InterfacePolymorphismTest extends MongoTest {
    @Test
    public void polymorphTest() throws Exception {
        MorphiumSingleton.get().dropCollection(IfaceTestType.class);
        IfaceTestType ifaceTestType = new IfaceTestType();
        ifaceTestType.setName("A Complex Type");
        ifaceTestType.setPolyTest(new SubClass(11));
        MorphiumSingleton.get().store(ifaceTestType);

        assert (MorphiumSingleton.get().createQueryFor(IfaceTestType.class).countAll() == 1);
        List<IfaceTestType> lst = MorphiumSingleton.get().createQueryFor(IfaceTestType.class).asList();
        for (IfaceTestType tst : lst) {
            log.info("Class " + tst.getClass().toString());
        }
    }

    @Entity
    public static class IfaceTestType {
        @Id
        private ObjectId id;
        private String name;
        private IPolyTest polyTest;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public IPolyTest getPolyTest() {
            return polyTest;
        }

        public void setPolyTest(IPolyTest polyTest) {
            this.polyTest = polyTest;
        }
    }

    @Embedded(polymorph = true)
    @NoCache
    public static interface IPolyTest {
        int getNumber();
    }

    public static class SubClass implements IPolyTest {
        private int number;

        private SubClass() {}

        public SubClass(int num) {
            number = num;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }
    }
}
