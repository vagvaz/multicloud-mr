/**
 * Created by tr on 21/4/2015.
 */
public class Replay {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Help, Replay args: ip:port path_of_data_keys prefixes webpagePrefixes delayms_per_put_tocache (multi)  \n(prefixes seperated with |)");
            System.out.println("Example java -jar data-replay-1.0-SNAPSHOT-jar-with-dependencies.jar 10.106.0.33:11222,10.106.0.33:11223 /home/ubuntu/snapshot6268/ test-nutchWebBackup default.webpages 36000");
            System.exit(0);
        }
        int delay = 1000;
        if (args.length >= 5)
            delay = Integer.parseInt(args[4]);
        System.out.println("Delay per put " + delay);
        boolean multi=false;
        if(args.length ==6)
            if(args[5].toLowerCase().equals("multi")){
                System.out.print("MultiCloud!!! " + args[0]);
                multi=true;
            }

        ReplayTool tool = new ReplayTool(args[1], args[3], args[2], args[0] ,multi);
        tool.setDelay(delay);
        tool.replayNutch(true);
    }
}
