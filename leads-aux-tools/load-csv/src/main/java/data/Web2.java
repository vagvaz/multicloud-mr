package data;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class Web2 {

    /**
     * @param domain - (String)
     *
     * @return PR rating (int) or -1 if unavailable or internal error happened.
     */
    public static int pagerank(String domain) {

        String result = "";

//        JenkinsHash jenkinsHash = new JenkinsHash();
//        long hash = jenkinsHash.hash(("info:" + domain).getBytes());
//        String url = "http://toolbarqueries.google.com/tbr?client=navclient-auto&hl=en&"
//                + "ch=6" + hash + "&ie=UTF-8&oe=UTF-8&features=Rank&q=info:" + domain;
//
//        try {
//            URLConnection conn = new URL(url).openConnection();
//
//            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//
//            String input;
//            while ((input = br.readLine()) != null) {
//                result = input.substring(input.lastIndexOf(":") + 1);
//            }
//
//        } catch (Exception e) {
//            System.err.println(" " +e.getMessage());
//        }

        if ("".equals(result)) {
            return -1;
        } else {
            return Integer.valueOf(result);
        }

    }

}
