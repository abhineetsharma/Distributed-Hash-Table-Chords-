import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.time.LocalTime;
import java.util.*;

public class ChordHandler implements FileStore.Iface {

    private int port;

    private List<NodeID> listOfNode;
    private Map<String, NodeID> uniqueFingerTableEntry;
    private List<String> hashKeys;

    public Map<String, RFile> fileMap = new HashMap<>();

    public ChordHandler(int portI) {
        port = portI;
    }


    private String getCurrentIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return null;
    }


    @Override
    public void writeFile(RFile rFile) throws TException {
        SystemException exception = null;
        RFile serverRFile = null;
        RFileMetadata serverMetadata = null;
        RFileMetadata userMetadata = null;
        String fileName = null;
        String contents = null;


        if (rFile != null) {
            userMetadata = rFile.getMeta();
            if (userMetadata != null) {
                fileName = userMetadata.getFilename();
                contents = rFile.getContent();


                if (fileMap != null && fileMap.containsKey(fileName)) {
                    serverRFile = fileMap.get(fileName);
                    serverMetadata = serverRFile.getMeta();

                    if (serverMetadata.getOwner().equals(userMetadata.getOwner())) {

                        serverRFile.setContent(contents);
                        serverMetadata.setVersion(serverMetadata.getVersion() + 1);

                        String shaKey = sha256(contents);
                        serverMetadata.setContentHash(shaKey);
                        serverRFile.setMeta(serverMetadata);
                        fileMap.put(fileName, serverRFile);
                    } else {
                        exception = new SystemException();
                        exception.setMessage("Error cant overwrite the file of a different owner.");
                        throw exception;
                    }
                } else {


                    rFile.setContent(contents).setMeta(userMetadata.setVersion(0).setContentHash(sha256(contents)));
                    fileMap.put(fileName, rFile);
                }
            }
        }
    }

    @Override
    public RFile readFile(String filename, String owner) throws TException {
        SystemException exception = null;
        RFile rFile = null;
        String key = filename;
        if ((fileMap.containsKey(key))) {
            rFile = fileMap.get(key);
            RFileMetadata metadata = rFile.getMeta();
            if (!metadata.getOwner().equals(owner)) {
                exception = new SystemException();
                exception.setMessage("This file is not owned by the specified user");
                throw exception;
            }
            return rFile;
        } else {
            exception = new SystemException().setMessage("File not found. Please enter valid file name");
            throw exception;
        }
    }

    @Override
    public void setFingertable(List<NodeID> node_list) throws TException {
        SystemException exception = null;
        listOfNode = node_list;
        uniqueFingerTableEntry = new LinkedHashMap<>();
        if (null != node_list && node_list.size() > 0) {
            for (NodeID node : node_list) {
                uniqueFingerTableEntry.put(node.id, node);
            }
        } else {
            exception = new SystemException();
            exception.setMessage("Error: Finger table null or empty");
            throw exception;
        }


        hashKeys = new ArrayList<>(uniqueFingerTableEntry.keySet());

    }

    String getMethodName() {
        return "";//LocalTime.now() + " " + Thread.currentThread().getStackTrace()[2].getMethodName();
    }


    @Override
    public NodeID findSucc(String key) throws TException {

        NodeID successNode = null;
        try {


            int currentPort = port;
            String ip = null;
            ip = String.format("%s:%d", getCurrentIp(), port);
            String currentHashKey = "";

            currentHashKey = sha256(ip);

            int result = currentHashKey.compareToIgnoreCase(key);

            if (result == 0) {
                successNode = new NodeID().setPort(currentPort).setId(getCurrentIp()).setId(currentHashKey);
            } else {
                NodeID predecessorNode = findPred(key);
                if (null == predecessorNode) {
                    //throw error
                    SystemException nullException = new SystemException();
                    nullException.setMessage("predecessor Node cannot be found");
                    throw new SystemException();
                } else {
                    int predecessorPort = predecessorNode.getPort();
                    if (currentPort == predecessorPort) {
                        successNode = this.getNodeSucc();
                    } else {
                        String host = predecessorNode.getIp();
                        int pPort = predecessorNode.getPort();
                        if (pPort == currentPort) {
                            successNode = this.getNodeSucc();
                        } else {
                            TSocket transport = null;
                            TBinaryProtocol protocol = null;
                            try {
                                transport = new TSocket(host, pPort);
                                transport.open();
                                try {
                                    FileStore.Client client = new FileStore.Client(new TBinaryProtocol(transport));
                                    successNode = client.getNodeSucc();
                                } catch (SystemException e) {
                                    throw e;
                                } catch (TException e) {
                                    e.printStackTrace();
                                    System.exit(1);
                                }

                            } catch (TTransportException e) {
                                e.printStackTrace();
                                System.exit(1);
                            } finally {
                                if (transport != null)
                                    transport.close();
                            }
                        }
                    }
                }

            }
        } catch (TException ex) {
            throw ex;
        }
        return successNode;
    }

    @Override
    public NodeID findPred(String key) throws TException {
        System.out.println(getMethodName());
        NodeID predecessorNode = null;
        try {


            int currentPort = port;
            String ip = String.format("%s:%d", getCurrentIp(), port);
            String currentHashKey = "";

            currentHashKey = sha256(ip);//MessageDigest.getInstance("SHA-256").digest(ip.getBytes(StandardCharsets.UTF_8)).toString();

            NodeID currentNode = new NodeID();
            currentNode.setIp(getCurrentIp());
            currentNode.setPort(currentPort);
            currentNode.setId(currentHashKey);

            if (listOfNode != null && listOfNode.size() > 0) {
                NodeID temp = listOfNode.get(0);
                String tempKey = temp.getId();

                if (between(key, currentHashKey, tempKey)) {
                    predecessorNode = currentNode;
                } else {
                    NodeID targetNode = getNodeFromNodeList(key);
                    try {
                        predecessorNode = RpcCallToNextNode(targetNode, key);
                    } catch (SystemException exception) {
                        throw exception;
                    }
                }
            } else {
                SystemException nullException = new SystemException();
                nullException.setMessage("node list is null");
                throw nullException;
            }


        } catch (TException ex) {
            ex.printStackTrace();
            throw ex;

        }
        return predecessorNode;
    }

    private NodeID getNodeFromNodeList(String key) {
        System.out.println(getMethodName());
        SystemException exception = null;
        NodeID targetNode = null;
        try {


            int currentPort = port;
            String ip = String.format("%s:%d", getCurrentIp(), port);

            String currentHashKey = sha256(ip);

            NodeID currentNode = new NodeID();
            currentNode.setIp(getCurrentIp());
            currentNode.setPort(currentPort);
            currentNode.setId(currentHashKey);

            int index = hashKeys.size();

            if (index == 0) {
                exception = new SystemException();
                exception.setMessage("Error: Finger table null or empty");
                throw exception;
            }
            while (index-- != 0) {
                targetNode = uniqueFingerTableEntry.get(hashKeys.get(index));
                String nodeHashKey = targetNode.getId();


                if (between(nodeHashKey, currentHashKey, key)) {
                    break;
                }
            }
        } catch (TException ex) {
            ex.printStackTrace();
            System.out.println("getNodeFromNodeList");
        }
        return targetNode;

    }

    private NodeID RpcCallToNextNode(NodeID targetNode, String key) throws TException {
        SystemException exception = null;
        if (targetNode.getPort() == port)
            return targetNode;
        NodeID predecessorNode = null;
        int currentPort = port;
        try {

            if (currentPort != targetNode.getPort()) {

                String host = targetNode.getIp();
                int targetPort = targetNode.getPort();

                try {
                    TSocket transport = new TSocket(host, targetPort);
                    transport.open();
                    try {
                        FileStore.Client client = new FileStore.Client(new TBinaryProtocol(transport));
                        predecessorNode = client.findPred(key);
                    } catch (SystemException e) {

                        e.setMessage("System exception occurred " + e.getMessage());
                        throw e;
                    } catch (TException e) {
                        e.printStackTrace();
                        throw e;
                    }
                    transport.close();
                } catch (TTransportException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
            } else {
                throw new SystemException().setMessage("Calling same port again, Last entry is same as current node.Infinit loop");
            }
        } catch (Exception ex) {
            exception = new SystemException();
            exception.setMessage("Error: Finger table null or empty");
            throw exception;
        }
        return predecessorNode;
    }

    private boolean between(String key, String from, String to) {
        if (from.compareToIgnoreCase(to) > 0)
            return key.compareToIgnoreCase(from) > 0 || key.compareToIgnoreCase(to) <= 0;
        else if (from.compareToIgnoreCase(to) < 0)
            return key.compareToIgnoreCase(from) > 0 && key.compareToIgnoreCase(to) <= 0;
        else return true;
    }

    @Override
    public NodeID getNodeSucc() throws TException {
        System.out.println(getMethodName());
        try {
            if (listOfNode.size() > 0) {
                return listOfNode.get(0);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static String sha256(String base) throws SystemException {

        try {
            byte[] hashArray = MessageDigest.getInstance("SHA-256").digest(base.getBytes("UTF-8"));
            StringBuilder hexString = new StringBuilder();

            for (byte hash : hashArray) {
                String hex = Integer.toHexString(0xff & hash);

                if (1 == hex.length()) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception ex) {
            SystemException exception = new SystemException();
            exception.setMessage("Error: is sha function");
            throw exception;
        }

    }
}

