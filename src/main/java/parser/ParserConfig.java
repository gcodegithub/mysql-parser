package parser;

/**
 * Created by hp on 14-9-28.
 */
public class ParserConfig {



    private String hbase;


    public ParserConfig(String hbase) {
        this.hbase = hbase;
    }


    public String getHbase() {
        return hbase;
    }

    public void setHbase(String hbase) {
        this.hbase = hbase;
    }
}
