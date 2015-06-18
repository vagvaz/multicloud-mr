package data;

import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.vertx.java.core.json.JsonObject;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by vagvaz on 05/13/15.
 */
public class LoadAmplab2 {
    enum plugs {SENTIMENT, PAGERANK};
    transient protected static Random r;

    static int delay = 0;
    static RemoteCacheManager remoteCacheManager = null;
    static InfinispanManager imanager = null;
    static EnsembleCacheManager emanager;

    static ConcurrentMap embeddedCache = null;
    static RemoteCache remoteCache = null;
    static EnsembleCache ensembleCache = null;
    static ArrayList<EnsembleCache>  ecaches = new ArrayList<>();
    static boolean ensemple_multi = false;
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        r = new Random(0);

        if (args.length == 0) {
            System.out.print(" Syntax:\tconvertadd filename {inputcollumn conversion}+ \n where convertion type: sentiment, pagerank");
            System.err.println("or  \t\t$prog loadIspn dir (delay per put)\n ");
            System.err.println("or  \t\t$prog loadRemote dir host port (delay per put)\n ");
            System.err.println("or  \t\t$prog loadEnsemble dir host:port(|host:port)+ (delay per put)\n ");
            System.err.println("or  \t\t$prog loadEnsembleMulti dir host:port(|host:port)+ (delay per put)\n ");

            System.exit(-1);
        }

        LQPConfiguration.initialize();

        if (args[0].startsWith("l")) {
            if(args[0].equals("loadIspn")) {
                imanager = InfinispanClusterSingleton.getInstance().getManager();

            }else if(args[0].equals("loadRemote")) {
                if (args.length != 2 && args.length < 4) {
                    System.err.println("wrong number of arguments for load $prog load dir/ $prog load dir host port (delay per put)");
                    System.exit(-1);
                }
                remoteCacheManager = createRemoteCacheManager(args[2], args[3]);
            }  else if(args[0].startsWith("loadEnsemble")){
                if ( args.length < 3) {
                    System.err.println("or  \t\t$prog loadEnsemble(Multi) dir host:port(|host:port)+ (delay per put)\n ");
                    System.exit(-1);
                }

                if(args[0].equals("loadEnsembleMulti"))
                    ensemple_multi=true;
                if (args.length == 4) {
                    delay = Integer.parseInt(args[3]);
                    System.out.println("Forced delay per put : " + delay + " ms");
                }
                String ensembleString = args[2];
                System.out.println("Using ensemble sring " + ensembleString);
                emanager = new EnsembleCacheManager((ensembleString));
                System.out.println("Emanager has " + emanager.sites().size() + " sites");
                emanager.start();
            }else{
                System.exit(0);
            }
            loadData(args[1],args[5], args[6]);
        }
    }



    private static void loadData(String path, String arg5, String arg6) throws IOException, ClassNotFoundException {
        Long startTime = System.currentTimeMillis();
        Path dir = Paths.get(path);
        List<File> files = new ArrayList<>();

        DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(Path file) throws IOException {
                return (Files.isDirectory(file));
            }
        };

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir,
                filter)) {
            for (Path path1 : stream) {
                // Iterate over the paths in the directory and print filenames
                //System.out.println(path1.getFileName());

                dir = Paths.get(path+"/"+path1.getFileName());

                try (DirectoryStream<Path> stream1 = Files.newDirectoryStream(dir, "{part}*")) {
                    for (Path entry : stream1) {
                        files.add(entry.toFile());
                    }
                } catch (IOException x) {
                    throw new RuntimeException(String.format("error reading folder %s: %s", dir, x.getMessage()), x);
                }
                for (File csvfile : files) {
                    System.out.print("Loading file: " + csvfile.getName());
                    Long filestartTime = System.currentTimeMillis();
                    loadDataFromFile(csvfile,arg5,arg6);
                    System.out.println("Loading time: " + DurationFormatUtils.formatDuration(System.currentTimeMillis() - filestartTime, "HH:mm:ss,SSS"));
                }
                System.out.println("Loading finished.");
                System.out.println("Overall Folder Loading time: " + DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss,SSS"));
                System.exit(0);



            }
        } catch (IOException e) {
            e.printStackTrace();
        }



    }

    private static void loadDataFromFile(File csvfile, String arg5, String arg6) throws IOException {
        String tableName = csvfile.getParentFile().getName();
        String keysFilename = csvfile.getAbsoluteFile().getParent() + "/" + tableName + ".keys";
        Path path = Paths.get(keysFilename);

        BufferedReader keyReader = null;
        if (Files.exists(path)) {
            try {
                keyReader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFilename)));
            } catch (FileNotFoundException e) {
                System.out.println("Unable to read keys file, skipping "+ tableName);
                e.printStackTrace();
                return;
            }
            System.out.println(" Loading key from file " + tableName + ".keys");
        } else {
            System.err.println(" No keys file, skipping " + tableName);
            return;
        }

        //Read the keys
        ArrayList<Class> columnType = new ArrayList<>();
        ArrayList<String> columns = new ArrayList<>();
        String[] primaryKeys = null;
        int[] primaryKeysPos = null;
        try {
            String keyLine = "";

            while ((keyLine = keyReader.readLine()) != null) {
                if (keyLine.startsWith("#col")) {
                    keyLine = keyReader.readLine();//Next line got keys
                    if (keyLine == null) {
                        System.err.print("No Column Key Data line after #collumnline");
                        return;
                    }
                    String[] keysTypePairs = keyLine.split(",");
                    {
                        System.out.print("Must find #" + keysTypePairs.length + " column names, ");
                        for (String keyTypePair : keysTypePairs) {
                            String[] pair = keyTypePair.trim().split("\\s+");
                            if (pair.length != 2) {
                                System.err.print("Column Key Data are not correct! Key line must be at ,Column name space ColumnType, form");
                                continue;
                            } else {
                                columns.add(pair[0]);
                                if (pair[1].toLowerCase().equals("text"))
                                    columnType.add(String.class);
                                else if (pair[1].toLowerCase().equals("bigint"))
                                    columnType.add(Long.class);
                                else if (pair[1].toLowerCase().equals("int"))
                                    columnType.add(Long.class);
                                else if (pair[1].toLowerCase().equals("float"))
                                    columnType.add(Float.class);
                                else {
                                    System.err.print("Column Key not recognized type: " + pair[1]);
                                    continue;
                                }
                            }
                        }
                        System.out.println("Recognized Columns #" + keysTypePairs.length);
                    }
                } else if (keyLine.toLowerCase().startsWith("#primary")) {//Read the primary keys
                    keyLine = keyReader.readLine();//Next line got primary keys
                    if (keyLine == null) {
                        System.err.print("No primary Key Data line after #primary");
                        return;
                    }
                    primaryKeys = keyLine.trim().split(",");
                    for (int i = 0; i < primaryKeys.length; i++)
                        primaryKeys[i] = primaryKeys[i].trim();

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (primaryKeys == null) {
            System.err.println("Unable to find primary keys not importing file !");
            return;
        }
        if (columnType.isEmpty()) {
            System.err.println("Unable to find column keys not importing file !");
            return;
        }
        int pos = 0;
        primaryKeysPos = new int[primaryKeys.length];

        for (int i = 0; i < primaryKeys.length; i++) {
            if (columns.contains(primaryKeys[i])) {
                primaryKeysPos[i] = columns.indexOf(primaryKeys[i]);
            } else {
                System.err.println("Oups primary key not among columns, stop importing");
                return;
            }
        }

        if (initialize_cache(tableName)){
            int numofEntries = 0;
            int lines = 0;
            String key="";
            System.out.println("Importing data ... ");
            long sizeE = 0;

            String keyLine = "";

            try {
                keyReader = new BufferedReader(new InputStreamReader(new FileInputStream(csvfile)));
            } catch (FileNotFoundException e) {
                System.out.println("Unable to read keys file, skipping "+ tableName);
                e.printStackTrace();
                return;
            }

            while ((keyLine = keyReader.readLine()) != null){
                JsonObject data = new JsonObject();

                // read line and values separated by commas
                String[] dataline = keyLine.split(",");

                for (pos = 0; pos < columns.size(); pos++) {
                    String fullCollumnName =  "default."+tableName+"." + columns.get(pos);
                    try {
                        if (columnType.get(pos) == String.class)
                            data.putString(fullCollumnName, dataline[pos]);
                        else if (columnType.get(pos) == Long.class)
                            data.putNumber(fullCollumnName, Long.parseLong(dataline[pos]));
                        else if (columnType.get(pos) == Integer.class)
                            data.putNumber(fullCollumnName,  Integer.parseInt(dataline[pos]));
                        else if (columnType.get(pos) == Float.class)
                            data.putNumber(fullCollumnName,  Float.parseFloat(dataline[pos]));
                        else{
                            System.err.println("Not recognised type, stop importing");
                            return;
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("Line: " + lines + "Parsing error, put random generated float number");
                        data.putNumber(fullCollumnName, nextFloat(-3, 3));
                    }
                }

                key = dataline[primaryKeysPos[0]];
                for (int i = 1; i < primaryKeysPos.length; i++) {
                    key += ":" + dataline[primaryKeysPos[i]];
                }

//                try {
//                    System.out.println("putting... pageURL:" + data.getField("default." + tableName + ".pageURL").toString() + " -- pageRank:" + data.getField("default." + tableName + ".pageRank").toString());
//                } catch(NullPointerException npe){
//                    System.out.println("putting... sourceIP:" + data.getField("default." + tableName + ".sourceIP").toString() + " -- destURL:" + data.getField("default." + tableName + ".destURL").toString());
//                }
                put(key, data.toString());

                try {
                    sizeE+=serialize(data).length;
                } catch (IOException e) {
                    e.printStackTrace();
                }

                numofEntries++;

                if (delay > 50) {
                    System.out.println("Cache put: " + numofEntries);
                }
                if (numofEntries % 100 == 0){
                    System.out.println("Imported: " + numofEntries+" -- size: "+sizeE);
                }
            }

            System.out.println("Totally Imported: " + numofEntries);
        }
    }

    public static byte[] serialize(JsonObject obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj.toString());
        return b.toByteArray();
    }

    public static int randInt(int min, int max) {
        Random rand = new Random();
        int randomNum = rand.nextInt((max - min) + 1) + min;
        return randomNum;
    }

    public static Long randLong() {
        long x = 1234567L;
        long y = 23456789L;
        Random r = new Random();
        long number = x+((long)(r.nextDouble()*(y-x)));
        return number;
    }

    public static String randSmallString() {
        char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < 50; i++) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        String randomString = sb.toString();
        return randomString;
    }

    public static String randBigString(int arg6) {
        char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < arg6; i++) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        String randomString = sb.toString();
        return randomString;
    }

    public static float nextFloat(float min, float max) {
        return min + r.nextFloat() * (max - min);
    }

    private static void put(String key, String value) {
        Tuple tuple = new Tuple(value);
        if (remoteCache != null)
            remoteCache.put(remoteCache.getName() + ":" + key, tuple);
        else if (embeddedCache != null)
            embeddedCache.put(((Cache) embeddedCache).getName() + ":" + key, tuple);
        else if (ensembleCache!=null)
            ensembleCache.put( ensembleCache.getName() + ":" + key, tuple);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static boolean initialize_cache(String tableName) {

        System.out.println(" Tablename: " + tableName + " Trying to create cache: " + StringConstants.DEFAULT_DATABASE_NAME + "." + tableName);
        if (remoteCacheManager != null)
            try {
                remoteCache = remoteCacheManager.getCache(StringConstants.DEFAULT_DATABASE_NAME + "." + tableName);
            } catch (Exception e) {
                System.err.println("Error " + e.getMessage() + " Terminating file loading.");
                return false;
            }
        else if (imanager != null)
            embeddedCache = imanager.getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + "." + tableName);
        else if (emanager != null)
            ensembleCache = emanager.getCache(StringConstants.DEFAULT_DATABASE_NAME + "." + tableName,new ArrayList<>(emanager.sites()),
                    EnsembleCacheManager.Consistency.DIST);
        else {
            System.err.println("Not recognised type, stop importing");
            return false;
        }


        if (embeddedCache == null && remoteCache == null && ensembleCache ==null) {
            System.err.println("Unable to create cache, exiting");
            System.exit(0);
        }
        return true;
    }

    private static RemoteCacheManager createRemoteCacheManager(String host, String port) {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addServer().host(host).port(Integer.parseInt(port));
        return new RemoteCacheManager(builder.build());
    }
}