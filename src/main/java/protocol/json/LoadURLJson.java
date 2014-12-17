package protocol.json;

import net.sf.json.JSONObject;
import parser.utils.ParserConfig;

/**
 * Created by hp on 14-11-14.
 */
public class LoadURLJson {

    public static void main(String[] args) {
        ParserConfig configer = new ParserConfig();
        MagpieConfigJson configJson = new MagpieConfigJson("jd-mysql-parser-1");
        JSONObject jRoot = configJson.getJson();
        if(jRoot != null) {
            JSONObject jContent = jRoot.getJSONObject("info").getJSONObject("content");
            configer.setHbaseRootDir(jContent.getString("HbaseRootDir"));
            configer.setHbaseDistributed(jContent.getString("HbaseDistributed"));
            configer.setHbaseZkQuorum(jContent.getString("HbaseZkQuorum"));
            configer.setHbaseZkPort(jContent.getString("HbaseZkPort"));
            configer.setDfsSocketTimeout(jContent.getString("DfsSocketTimeout"));
        }

        System.out.println(configer.getHbaseRootDir()+","+configer.getHbaseDistributed()+"," +
                configer.getHbaseZkQuorum()+","+configer.getHbaseZkPort()+","+configer.getDfsSocketTimeout());

    }

}
