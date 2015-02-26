import junit.framework.Assert;
import org.apache.bigtop.load.LoadGen;

import java.io.File;
import java.util.Locale;

/**
 * Created by jayunit100 on 2/25/15.
 */
public class TestLoadGen {

    @org.junit.Test
    public void test(){
        new File("/tmp/transactions0.txt").delete();
        LoadGen.TESTING=true;
        LoadGen.main(new String[]{"/tmp","1","5","1500000","13241234"});
        Assert.assertTrue(new File("/tmp/transactions0.txt").length()>0);
    }
}
