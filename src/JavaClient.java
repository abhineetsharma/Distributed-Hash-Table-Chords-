
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;


import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import java.math.BigInteger;

public class JavaClient {
    static int serverPort;
    static String serverIp;
    static String user;
    static String fileName;

    enum Operation {Write, Read}

    static String getByKey(String key, String[] sarr) {
        for (int i = 0; i < sarr.length - 1; i++) {
            if (sarr[i].toLowerCase().contains(key.toLowerCase()))
                return sarr[i + 1];
        }
        return null;
    }

    public static void main(String[] args) {

        // if (args.length != 3) {
        // System.out.println("Please enter simple/secure [ip] [port]");
        // System.exit(0);
        // }
        //./client localhost 9090 --operation write --filename test3.txt --user shrikant

        if (args.length != 8) {
            System.out.println("Please enter the parameters correctly example: ./client localhost 9090 --operation write --filename test3.txt --user abhineet");
            System.exit(1);
        }
        serverIp = args[0];
        serverPort = Integer.parseInt(args[1]);
        Operation operation = getByKey("--operation", args).equalsIgnoreCase(Operation.Write.toString()) ?
                Operation.Write
                : Operation.Read;
        user = getByKey("--user", args);
        fileName = getByKey("--filename", args);
        System.out.println(serverIp + " " + serverPort + " " + user + " " + fileName);
        try {
            TTransport transport;
            // transport = new TSocket("simple", Integer.valueOf("9091"));
            transport = new TSocket(serverIp, serverPort);

            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            FileStore.Client client = new FileStore.Client(protocol);

            perform(client, operation, transport);
            //transport.close();

        } catch (TException x) {
            x.printStackTrace();
        }
    }

    private static void perform(FileStore.Client client, Operation operation, TTransport transport) throws TException {
        String key = null;
        key = ChordHandler.sha256( user+ ":" + fileName);
        NodeID succNode = client.findSucc(key);
        transport.close();
        System.out.println("=======================================================");
        System.out.println("KEY : " + key);
        System.out.println("=======================================================");
        System.out.println("Node ID : " + succNode.id + ": NODE IP : " + succNode.ip + "NODE PORT : " + succNode.port);

        if (succNode != null) {
            int port = succNode.getPort();
            String ip = succNode.getIp();
            TSocket newTransport = new TSocket(ip, port);
            newTransport.open();
            TBinaryProtocol protocol = new TBinaryProtocol(newTransport);

            client = new FileStore.Client(protocol);

            switch (operation) {
                case Write:
                    RFile rFile = new RFile();
                    RFileMetadata metadata = new RFileMetadata();
                    String contents = "Abhineet Sharma\n";
                    if (contents.equals("")) {
                        System.out.println("File does not exists in local directory");
                        System.exit(0);
                    }
                    rFile.setContent(contents);
                    metadata.setOwner(user);
                    metadata.setFilename(fileName);
                    rFile.setMeta(metadata);
                    client.writeFile(rFile);
                    newTransport.close();
                    break;
                case Read:
                    rFile = client.readFile(fileName, user);
                    System.out.println(rFile.content);
                    System.out.println(rFile.meta);
                    newTransport.close();
                    break;
            }

        }
    }
}