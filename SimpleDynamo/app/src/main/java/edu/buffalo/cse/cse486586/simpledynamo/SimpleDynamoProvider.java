package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static String received_all = null;
    static int count_received_all = 0;
    static long starttime1 ;
    static long endtime1 ;
    static String received_all_test = null;
    static int count_received_all_test = 0;
    static long starttime1_test ;
    static long endtime1_test ;

    private class node {

        String node_val;
        node predecessor;
        node succesor;

        node(String node_val) {
            this.node_val = node_val;
            predecessor = null;
            succesor = null;
        }

        void setnext(node entry) {
            this.succesor = entry;
        }

        void setprev(node entry) {
            this.predecessor = entry;
        }

    }
    private class CircularLinkedlist {
        node first;
        node end;
        int size;

        public CircularLinkedlist() {
            first = null;
            end = null;
            size = 0;
        }

        public void insert(String val) {
            node entry = new node(val);
            if (first == null) {
                entry.setnext(entry);
                entry.setprev(entry);
                first = entry;
                end = first;
            } else {
                entry.setprev(end);
                end.setnext(entry);
                first.setprev(entry);
                entry.setnext(first);
                end = entry;
            }
            size = size + 1;
        }

    }

    static final String TAG = "My_Tag";
    static final int SERVER_PORT = 10000;
    static ConcurrentHashMap<Integer, String> hash = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, String> local_key_val = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, String> replicate_hash = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, String> local_private = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, String> query_storage = new ConcurrentHashMap<>();
    static HashSet<String> set = new HashSet<>();
    static int flag_val = 0;
    int seq = 0;
    static String array[] = new String[2];
    static String array_rep[] = new String[2];

    static boolean flag = false;

    static boolean flag_replication = false;
    static boolean flag_all = false;
    static Uri Muri;
    static boolean flag_all_test = false;


    CircularLinkedlist chord_new = new CircularLinkedlist();


    private void getSortedOrder() {

        String arr[] = {"5562", "5556", "5554", "5558", "5560"};
        for (int i = 0; i < 5; i++) {
            if (set.contains(arr[i])) {
                chord_new.insert(arr[i]);
            }
        }

        flag_val = 1;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.d(" present in delete", TAG);
        String key = selection;
//        getContext().deleteFile(key);
        local_key_val.remove(key);
        query_storage.remove(key);
        replicate_hash.remove(key);
        local_private.remove(key);
        new ClientTaskdeleteit().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, getport());
        seq = seq - 1;
        return 0;
    }


    private class ClientTaskdeleteit extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String msgReceived = msgs[0];

            try {

                for (int i = 0; i < 5; i++) {
                    Integer arr[] = {5562*2,5556*2,5554*2,5558*2,5560*2};
                    int remotePort1 = arr[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (remotePort1));

                    String msgsend = msgReceived + "%" + "delete_it";
                    Log.e("inside delete it "+msgsend,"check this");
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgsend);
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public String getport() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(SimpleDynamoActivity.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myport = String.valueOf((Integer.parseInt(portStr) * 2));
        return myport;
    }

    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

//        long start_test = System.currentTimeMillis();
//        long end_test = System.currentTimeMillis();
//
//        while(true)
//        {
//            end_test = System.currentTimeMillis();
//            if(end_test-start_test >=2200)
//            {
//                break;
//            }
//        }


        String key = values.get("key").toString();
            String val = values.get("value").toString();

            String myport = getport();
            String insert_here = getposition(key);

            String insert_her = Integer.toString(Integer.parseInt(insert_here) * 2);
            String entry = key+"%"+val;
            new ClientTaskSaveit().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, entry, myport);

        if (insert_her.equals(myport)) {
                Log.e("equals ", TAG);
                String args[] = {key, val};
                insert_tosave(args);
                // to replicate it into two other modify here
                Log.e("Insert here is executed ", key + "%%%%%%%%%%%%%%" + val);
                local_private.put(key, val);
                local_key_val.put(key,val);
                query_storage.put(key, val);
                String rep_nodes = getnodestoreplicate(insert_here);
                String replicate_send = key + "%" + val + "%" + rep_nodes + "%" + insert_her;
                new ClientTaskReplicate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replicate_send, myport);

            } else {
                String send = key + "%" + val + "%" + insert_her;
                Log.e("sending the actual place where it is to be inserted", send);
                new ClientTaskinsert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send, myport);
                query_storage.put(key, val);

                String rep_nodes = getnodestoreplicate(insert_here);
                String replicate_send = key + "%" + val + "%" + rep_nodes + "%" + insert_her;

                Log.e("Next Replicationnn**********", TAG);
                new ClientTaskReplicate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replicate_send, myport);
            }
        return null;
    }

    private class ClientTaskReplicate extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String msgReceived = msgs[0];

            String[] msgs_parts = msgReceived.split("%");
            // key+"%"+val+"%"+node[1]+"%"+node[2]

            try {

                for (int i = 2; i < 4; i++) {
                    Log.e("In replication key + val + to which ports sending replicate", msgReceived);
                    int remotePort1 = Integer.parseInt(msgs_parts[i]) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (remotePort1));

                    String msgsend = msgs_parts[0] + "%" + msgs_parts[1] + "%" + msgs_parts[4] + "%" + "replicate";
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgsend);
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }


    private class ClientTaskSaveit extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String msgReceived = msgs[0];

            String[] msgs_parts = msgReceived.split("%");
            // key+"%"+val

            try {

                for (int i = 0; i < 5; i++) {
                    Integer arr[] = {5562*2,5556*2,5554*2,5558*2,5560*2};
                    int remotePort1 = arr[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (remotePort1));

                    String msgsend = msgs_parts[0] + "%" + msgs_parts[1] + "%" + "querystorage";
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgsend);
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }


    public String getnodestoreplicate(String input) {

        //
//        String arr[] = {"5562","5556","5554","5558","5560"};
        if (input.equals("5562"))
            return "5556" + "%" + "5554";
        if (input.equals("5556"))
            return "5554" + "%" + "5558";
        if (input.equals("5554"))
            return "5558" + "%" + "5560";
        if (input.equals("5558"))
            return "5560" + "%" + "5562";
        if (input.equals("5560"))
            return "5562" + "%" + "5556";
        return null;
    }


//    public String getprevnodes(String input) {
//
//        //
////        String arr[] = {"5562","5556","5554","5558","5560"};
//        if (input.equals("5562"))
//            return "5560" + "%" + "5558";
//        if (input.equals("5556"))
//            return "5562" + "%" + "5560";
//        if (input.equals("5554"))
//            return "5556" + "%" + "5562";
//        if (input.equals("5558"))
//            return "5554" + "%" + "5556";
//        if (input.equals("5560"))
//            return "5554" + "%" + "5558";
//        return null;
//    }

    private class ClientTaskinsert extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String msgReceived = msgs[0];

            String[] msgs_parts = msgReceived.split("%");

            try {
                String remotePort1 = msgs_parts[2];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort1));
                Log.e(" in client task insert key + val + sending msg ", msgReceived);
                String msgsend = msgs_parts[0] + "%" + msgs_parts[1] + "%" + "insert";
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgsend);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

    public String getposition(String keyval) {

        if (flag_val == 0) {
            getSortedOrder();
        }

        node temp_first = chord_new.first;
        String key = keyval;
        String insert_position = null;

        try {
            {

                node temp = chord_new.first;
                while (temp != chord_new.end) {

                    if (genHash(key).compareTo(genHash(temp.node_val)) > 0 && genHash(key).compareTo(genHash(temp.succesor.node_val)) <= 0) {
                        insert_position = temp.succesor.node_val;
                        Log.e((key), insert_position);
                        return insert_position;
                    }

                    temp = temp.succesor;

                }
            }
            return temp_first.node_val;

        } catch (NoSuchAlgorithmException e) {

        }
        return null;
    }

    public Uri insert_tosave(String[] args) {

        Muri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        String key = args[0];
        String val = args[1];
        Uri uri1 = Muri.withAppendedPath(Muri, key);
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(SimpleDynamoActivity.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        Log.e(portStr + "got to save insert_tosave in this avd" + key, TAG);
        hash.put(seq, key);
        local_key_val.put(key, val);
        seq = seq + 1;
        return uri1;
    }

    @Override
    public boolean onCreate() {

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
        }

        String myport = getport();
        Log.e("step 1 of sequence", TAG);

        String arr[] = {"5562", "5556", "5554", "5558", "5560"};


        for (int i = 0; i < 5; i++) {
            set.add(arr[i]);
        }

        String test = "test";


       new ClientTasktest1().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, test, myport);
    //   new ClientTasktest().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, test, myport);

        return true;

    }


    private class ClientTasktest extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            try {
                for (int i = 0; i < 5; i++)
                {

                    String arr[] = {"5562", "5556", "5554", "5558", "5560"};

                    int port_vals = Integer.parseInt(arr[i]) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port_vals);
                    String msgToSend = getport() + "%" + "testsendall";
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgToSend);
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

    private class ClientTasktest1 extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            try {
                for (int i = 0; i < 5; i++)
                {
//                    String myport = getport();
//                    String mynode = Integer.toString((Integer.parseInt(myport))/2);
//                    String getrep = getnodestoreplicate(mynode);
//                    String reps[] =getrep.split("%");
//                    String porttosend = reps[1];
//                    Integer sendto = Integer.parseInt(porttosend)*2;

                    String arr[] = {"5562", "5556", "5554", "5558", "5560"};

                    int sendto = Integer.parseInt(arr[i]) * 2;


                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            sendto);
                    String msgToSend = getport() + "%" + "test1sendall";
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgToSend);
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            if (serverSocket != null) {
                while (true) {
                    BufferedReader input = null;
                    String line;
                    try {
                        Socket clientSocket = serverSocket.accept();
                        input = new BufferedReader(
                                new InputStreamReader(clientSocket.getInputStream()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {

                        line = input.readLine();

                        String[] line_parts = line.split("%");


                        if (line_parts.length == 2) {
                            if (line_parts[1].equals("delete_it")) {
                                local_key_val.remove(line_parts[0]);
                                query_storage.remove(line_parts[0]);
                                Log.e("Inside server delete","check this");
                                replicate_hash.remove(line_parts[0]);
                                local_private.remove(line_parts[0]);
                                seq = seq -1;
                            }
                        }

                        if (line_parts[line_parts.length - 1].equals("retrnreplication")) {
                            for (int i = 2; i < line_parts.length - 2; i++) {
                                // 1 -port 2 - null 3 - last - return replication
                                Log.e("got reply for return replication ###############", line_parts[i]);
                                String keyvalpair = line_parts[i];
                                String keyval[] = keyvalpair.split("#");
                                String args[] = {keyval[0], keyval[1]};
                                local_private.put(keyval[0], keyval[1]);
                                insert_tosave(args);
                            }
                        }

                        if (line_parts[line_parts.length - 1].equals("thisistest")) {

                            Log.e(" this is inside test +++++++++++++++++++++++ ", line);
                            for (int i = 1; i < line_parts.length - 1; i++) {
                                String keyvalpair = line_parts[i];
                                String keyval[] = keyvalpair.split("#");
                                String args[] = {keyval[0], keyval[1]};

                                String positionitshldbe = getposition(keyval[0]);
                                String isitareplicate = getnodestoreplicate(positionitshldbe);

                                String port_positionitshldbe = Integer.toString(Integer.parseInt(positionitshldbe) * 2);
                                query_storage.put(keyval[0],keyval[1]);
                                if (port_positionitshldbe.equals(getport())) {
                                    local_private.put(keyval[0], keyval[1]);
                                    insert_tosave(args);
                                }

                                String isreplicates[] = isitareplicate.split("%");

                                for (int k = 0; k < 2; k++) {
                                    String port = Integer.toString(Integer.parseInt(isreplicates[k]) * 2);
                                    if (port.equals(getport())) {
                                        insert_tosave(args);
                                    }
                                }
                            }
                        }

                        if (line_parts[line_parts.length - 1].equals("queryatrep")) {

                                String portfromwch = line_parts[0];
                                String key = line_parts[1];

                                String val = query_storage.get(key);

                                String sendback =portfromwch+"%"+key+"%"+val+"%"+"queryatrepres";
                                new ClientTaskQueryRepReply().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendback, getport());

                        }


                            if (line_parts[line_parts.length - 1].equals("thisistest1")) {

                            Log.e(" this is inside test1 +++++++++++++++++++++++ ", line);
                            for (int i = 1; i < line_parts.length - 1; i++) {
                                String keyvalpair = line_parts[i];
                                String keyval[] = keyvalpair.split("#");
                                String args[] = {keyval[0], keyval[1]};

                                String positionitshldbe = getposition(keyval[0]);
                                String isitareplicate = getnodestoreplicate(positionitshldbe);

                                String port_positionitshldbe = Integer.toString(Integer.parseInt(positionitshldbe) * 2);
                                query_storage.put(keyval[0],keyval[1]);
                                if (port_positionitshldbe.equals(getport())) {
                                    local_private.put(keyval[0], keyval[1]);
                                    insert_tosave(args);
                                }

                                String isreplicates[] = isitareplicate.split("%");

                                for (int k = 0; k < 2; k++) {
                                    String port = Integer.toString(Integer.parseInt(isreplicates[k]) * 2);
                                    if (port.equals(getport())) {
                                        insert_tosave(args);
                                    }
                                }
                            }
                        }

//                        if (line_parts[line_parts.length - 1].equals("thisisfinalres")) {
//
//                            Log.e(" this is inside finl res hkjhkh testhffffffffffffffffffffffffffkkkkkkkkkkkkkk +++++++++++++++++++++++ ", line);
//                            for (int i = 1; i < line_parts.length - 1; i++) {
//                                String keyvalpair = line_parts[i];
//                                String keyval[] = keyvalpair.split("#");
//                                String args[] = {keyval[0], keyval[1]};
//
//                                String positionitshldbe = getposition(keyval[0]);
//                                String isitareplicate = getnodestoreplicate(positionitshldbe);
//
//                                String port_positionitshldbe = Integer.toString(Integer.parseInt(positionitshldbe) * 2);
//                                query_storage.put(keyval[0],keyval[1]);
//                                if (port_positionitshldbe.equals(getport())) {
//                                    local_private.put(keyval[0], keyval[1]);
//                                    insert_tosave(args);
//                                }
//
//                                String isreplicates[] = isitareplicate.split("%");
//
//                                for (int k = 0; k < 2; k++) {
//                                    String port = Integer.toString(Integer.parseInt(isreplicates[k]) * 2);
//                                    if (port.equals(getport())) {
//                                        insert_tosave(args);
//                                    }
//                                }
//                            }
//                        }

                        if (line_parts[line_parts.length - 1].equals("retrnstored")) {
                            for (int i = 2; i < line_parts.length - 3; i++) {
                                String whichport = line_parts[line_parts.length - 2];
                                // 1 -port 2 - null 3 - last - return replication
                                Log.e("got reply for return stored values ###############", line_parts[i]);
                                String keyvalpair = line_parts[i];
                                String keyval[] = keyvalpair.split("#");
                                String args[] = {keyval[0], keyval[1]};
                                replicate_hash.put(keyval[0] + "#" + keyval[1], whichport);
                                insert_tosave(args);
                            }
                        }

                        if (line_parts.length == 3) {
                            if (line_parts[2].equals("insert")) {
                                String args[] = {line_parts[0], line_parts[1]};
                                Log.e("in insert ", getport() + line_parts[0]);
                                local_private.put(line_parts[0], line_parts[1]);
                                insert_tosave(args);
                            }
                        }



                        if (line_parts.length == 3) {
                            if (line_parts[2].equals("querystorage")) {
                                query_storage.put(line_parts[0], line_parts[1]);
                            }
                        }

                        if (line_parts.length == 4) {
                            if (line_parts[3].equals("replicate")) {
                                String args[] = {line_parts[0], line_parts[1]};
                                Log.e("in replicate +++++++++++++++++++++++", line);
                                replicate_hash.put(line_parts[0] + "#" + line_parts[1], line_parts[2]);
                                insert_tosave(args);
                            }
                        }


                        if (line_parts.length == 2) {
                            if (line_parts[1].equals("sendall")) {
                                Log.e("Received req to send all", TAG);
                                String sendthis = line_parts[0] + "%" + line_parts[1];
                                Log.e("calling another client query :", sendthis);
                                new ClientTaskQueryAllRep().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendthis, getport());
                            }
                        }

                        if (line_parts.length == 2) {
                            if (line_parts[1].equals("sendalltest")) {
                                Log.e("Received req to send all", TAG);
                                String sendthis = line_parts[0] + "%" + line_parts[1];
                                Log.e("calling another client query :", sendthis);
                                new ClientTaskQueryAllReptest().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendthis, getport());
                            }
                        }

                        if (line_parts.length == 2) {
                            if (line_parts[1].equals("testsendall")) {
                                // "thisistest";
                                new ClientTaskQueryTestSendall().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, line, getport());
                            }
                        }

                        if (line_parts.length == 2) {
                            if (line_parts[1].equals("test1sendall")) {
                                // "thisistest";
                                new ClientTaskQueryTest1Sendall().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, line, getport());
                            }
                        }


//                        if (line_parts.length == 2) {
//                            if (line_parts[1].equals("testonesendall")) {
//                                // "thisistest";
//                                new ClientTaskQueryTestOneSendall().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, line, getport());
//                            }
//                        }

                        if (line_parts.length == 2) {

                            if (line_parts[1].equals("sendmyreplicates")) {
                                Log.e("Receivied in server mama ++++++++++++++++", line);
                                publishProgress(line);
                            }

                        }


                        if (line_parts.length == 2) {

                            if (line_parts[1].equals("sendyourstored")) {
                                Log.e("Receivied in server mama ++++++++++++++++ send your storeddd", line);
                                publishProgress(line);
                            }

                        }

                        if (line_parts.length == 3) {
                            //portekadanunchivachindo+"%"+key+"query"
                            if (line_parts[2].equals("query")) {

                                {
                                    String sendthis = line_parts[0] + "%" + line_parts[1];
                                    new ClientTaskQueryRep().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendthis, getport());

                                }

                            }
                        }

                        if (line_parts[line_parts.length - 1].equals("query_res_all")) {
                            // reading string from all
                            Log.e(" received all queries query_res_all", Integer.toString(count_received_all));
                            count_received_all = count_received_all + 1;
                            while (true) {
                                if (count_received_all <= set.size()) {
                                    for (int i = 1; i < line_parts.length - 1; i++) {
                                        received_all = received_all + "%" + line_parts[i];
                                    }
                                }
                                endtime1 = System.currentTimeMillis();
                                long totaltime1 = -starttime1 +endtime1;
                                Log.e(" THis is the time taking macha!@@@@@@@@@@@@@@@@@@@@@################",Long.toString(totaltime1));
                                if(totaltime1 >= 2000 && count_received_all == 4)
                                {
//                                    Log.e("the value is set #&^$&#^$&#^$&#^*&$^#*&$^#*&^$*#^$*##^(*#&(", "settingg flag_all value in this ");
                                    flag_all = true;
                                    break;
                                }
                                break;
                            }

                            //    Log.e("recevied all is and flag_all is true",received_all);
                            Log.e(" received all queries query_res_all", Integer.toString(count_received_all));
                            if (count_received_all == set.size() ) {
                                flag_all = true;
                            }
                        }

                        if (line_parts[line_parts.length - 1].equals("query_res_all_test")) {
                            // reading string from all
                            Log.e(" received all queries query_res_all", Integer.toString(count_received_all));
                            count_received_all_test = count_received_all_test + 1;
                            while (true) {
                                if (count_received_all_test <= set.size()) {
                                    for (int i = 1; i < line_parts.length - 1; i++) {
                                        received_all_test = received_all_test + "%" + line_parts[i];
                                    }
                                }
                                endtime1_test = System.currentTimeMillis();
                                long totaltime1_test = -starttime1_test + endtime1_test;
                                if (totaltime1_test >= 2000 && count_received_all_test == 4) {
//                                    Log.e("the value is set #&^$&#^$&#^$&#^*&$^#*&$^#*&^$*#^$*##^(*#&(", "settingg flag_all value in this ");
                                    flag_all_test = true;
                                    break;
                                }
                                break;
                            }
                        }

                        if (line_parts.length == 3) {//+matr_key+"%"+matr_val+"query_res"
                            if (line_parts[2].equals("query_res")) {
                                Log.e("query reply received ", line);
                                flag = true;
                                array[0] = line_parts[0];
                                array[1] = line_parts[1];
                            }
                        }


                        if (line_parts.length == 3) {//+matr_key+"%"+matr_val+"query_res"
                            if (line_parts[2].equals("queryreplicationreply")) {
                                Log.e("query reply received ", line);
                                flag_replication = true;
                                array_rep[0] = line_parts[0];
                                array_rep[1] = line_parts[1];
                            }
                        }

                    } catch (IOException e) {
                        e.printStackTrace();

                    }
                }
            }
            return null;
        }


        protected void onProgressUpdate(String... strings) {

            String myport = getport();

            String strReceived = strings[0].trim();
            String parts[] = strReceived.split("%");

            if (parts.length == 2 && parts[1].equals("sendmyreplicates")) {

                String porttosend = parts[0];
                String rep_msgs = null;
                Log.e("this is in publishprogress ..... +++++++ ", strReceived);
                Iterator it = replicate_hash.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();
                    if (pair.getValue().equals(porttosend)) {
                        rep_msgs = rep_msgs + "%" + pair.getKey();
                    }
                }
                String tosend = porttosend + "%" + rep_msgs;
                Log.e(" got the replicates to send back so sending ++++++++++++++++ ", tosend);
                new ClientTaskSendBackRepValues().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, tosend, getport());

            }


            if (parts.length == 2 && parts[1].equals("sendyourstored")) {
                String porttosend = parts[0];
                String msg = null;
                Iterator it = local_private.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();
                    if (pair.getValue().equals(porttosend)) {
                        msg = msg + "%" + pair.getKey() + "#" + pair.getValue();
                    }
                }
                String sendthis = porttosend + "%" + msg;


                Log.e("sending stored values back _________________", sendthis);
                new ClientTaskSendBackStored().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendthis, getport());
            }


            if (parts.length == 4) {
                new ClientTaskProgesss().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strReceived, myport);
            }

        }
    }


    private class ClientTaskSendBackStored extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived = msgs[0];
            String partsofmsg[] = strReceived.split("%");
            try {
                {
                    Log.e(" client task sending back rep values ", strReceived);
                    String remotePort1 = partsofmsg[0];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort1));
                    String msgToSend = strReceived + "%" + getport() + "%" + "retrnstored";
                    Log.e("sending back to port stored vlaues ", msgToSend);
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgToSend);
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }


    private class ClientTaskQueryRepReply extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived = msgs[0];
            String partsofmsg[] = strReceived.split("%");
            try {
                {
                    Log.e(" client task sending back rep values ", strReceived);
                    String remotePort1 = partsofmsg[0];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort1));
                    String msgToSend = partsofmsg[1] + "%" + partsofmsg[2] + "%" + "queryreplicationreply";
                    Log.e("sending back to port stored vlaues ", msgToSend);
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgToSend);
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private class ClientTaskQueryAllRep extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived = msgs[0];
            String parts[] = strReceived.split("%");

            try {
                //line_parts[0]+"%"line_parts[1]"
                String result_to_send = query_local_string_all();
                //     Log.e("Query reply sending backing to port  "+parts[0],result_to_send);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(parts[0]));
                String msgToSend = result_to_send + "%" + "query_res_all";
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgToSend);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private class ClientTaskQueryAllReptest extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived = msgs[0];
            String parts[] = strReceived.split("%");

            try {
                String result_to_send = query_local_string_all();
                //     Log.e("Query reply sending backing to port  "+parts[0],result_to_send);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(parts[0]));
                String msgToSend = result_to_send + "%" + "query_res_all_test";
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgToSend);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private class ClientTaskQueryTestSendall extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived = msgs[0];
            String parts[] = strReceived.split("%");

            try {
                //line_parts[0]+"%"line_parts[1]"
                String result_to_send = query_local_string_all();
                //     Log.e("Query reply sending backing to port  "+parts[0],result_to_send);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(parts[0]));
                String msgToSend = result_to_send + "%" + "thisistest";
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgToSend);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }
//
private class ClientTaskQueryTest1Sendall extends AsyncTask<String, Void, Void> {
    @Override
    protected Void doInBackground(String... msgs) {

        String strReceived = msgs[0];
        String parts[] = strReceived.split("%");

        try {
            //line_parts[0]+"%"line_parts[1]"
            String result_to_send = storage_string_all();
            //     Log.e("Query reply sending backing to port  "+parts[0],result_to_send);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(parts[0]));
            String msgToSend = result_to_send + "%" + "thisistest1";
            PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
            toServer.println(msgToSend);
            socket.close();

        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException");
        }

        return null;
    }
}

//    private class ClientTaskQueryTestOneSendall extends AsyncTask<String, Void, Void> {
//        @Override
//        protected Void doInBackground(String... msgs) {
//
//            String strReceived = msgs[0];
//            String parts[] = strReceived.split("%");
//
//            try {
//                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                        Integer.parseInt(parts[0]));
//
//                Iterator it = query_storage.entrySet().iterator();
//                String test = null;
//                while (it.hasNext()) {
//                    Map.Entry pair = (Map.Entry)it.next();
//                    test = test +"%"+ pair.getKey().toString()+"#"+ pair.getValue().toString();
//                }
//                test = test + "thisisfinalres";
//                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
//                toServer.println(test);
//                socket.close();
//
//            } catch (UnknownHostException e) {
//                Log.e(TAG, "ClientTask UnknownHostException");
//            } catch (IOException e) {
//                Log.e(TAG, "ClientTask socket IOException");
//            }
//
//            return null;
//        }
//    }
//
//



    private class ClientTaskQueryRep extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived = msgs[0];
            String parts[] = strReceived.split("%");

            try {
                //line_parts[0]+"%"line_parts[1]"
                String result_to_send = query_local_string(parts[1]);
                Log.e("Query reply sending to port " + parts[0], result_to_send);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(parts[0]));
                String msgToSend = result_to_send + "%" + "query_res";
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgToSend);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private class ClientTaskProgesss extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived = msgs[0];
            String parts[] = strReceived.split("%");
            String key = parts[0];
            String val = parts[1];
            String position = parts[2];
            String flag = parts[3];

            try {

                Integer remotePort1 = (Integer.parseInt(position) * 2);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        (remotePort1));

                String msgToSend = key + "%" + val + "%" + flag;
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgToSend);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

    private class ClientTaskSendBackRepValues extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived = msgs[0];
            String partsofmsg[] = strReceived.split("%");
            try {
                {
                    Log.e(" client task sending back rep values ", strReceived);
                    String remotePort1 = partsofmsg[0];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort1));
                    String msgToSend = strReceived + "%" + "retrnreplication";
                    Log.e("sending back to port replication vlaues ", msgToSend);
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgToSend);
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    public synchronized Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                                     String sortOrder) {
        // TODO Auto-generated method stub
        String key = selection;

        long start_test = System.currentTimeMillis();
        long end_test = System.currentTimeMillis();
        while(true)
        {
            end_test = System.currentTimeMillis();
            if(end_test-start_test >=200)
            {
                break;
            }
        }

        {
            if (selection.contains("@")) {

                return query_getall();

            } else if (selection.contains("*") && set.size() != 1) {
                String arr[] = {"key", "value"};
                MatrixCursor matrixCursor = new MatrixCursor(arr);
                Iterator it = query_storage.entrySet().iterator();

                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry)it.next();
                    String rows[] = {pair.getKey().toString(), pair.getValue().toString()};
                    matrixCursor.addRow(rows);
                }
                return matrixCursor;
            }

            else if (local_key_val.containsKey(key) && !local_key_val.get(key).equals(null)) {
                return query_local(selection);
            }

             else if(query_storage.containsKey(key) && !query_storage.get(key).equals(null))
            {
                String arr[] = {"key", "value"};
                Log.e("Inside query storage sasas asdasdasda da reply ", array[0] + "$$$$$$$$$$$$$$$$$" + array[1]);
                MatrixCursor matrixCursor = new MatrixCursor(arr);
                String rows[] = {key, query_storage.get(key)};
                matrixCursor.addRow(rows);
                return matrixCursor;
            }

             else {
                String querpos =  getposition(key);
                String querypor = Integer.toString(Integer.parseInt(querpos) * 2);
                {
                    String que = querypor + "%" + key + "%" + "query";
                    long start = System.currentTimeMillis();
                    new ClientTaskquery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, que, getport());
                    String query_replicate_pos = getnodestoreplicate(querpos);

                    while (true) {
                        long end = System.currentTimeMillis();
                        if (flag == true && end-start<=2000) {
                            String arr[] = {"key", "value"};
                            Log.e("Inside query reply ", array[0] + "$$$$$$$$$$$$$$$$$" + array[1]);
                            MatrixCursor matrixCursor = new MatrixCursor(arr);
                            String rows[] = {array[0], array[1]};
                            matrixCursor.addRow(rows);
                            flag = false;

                            return matrixCursor;

                        }

                        if(!(flag == true && end-start<=2000))
                        {   break;
                        }
                    }

                    String que_r = query_replicate_pos + "%" + key + "%" + "query";

                    new ClientTaskqueryatrep().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, que_r, getport());

                    while (true) {

                        if (flag_replication == true) {
                            String arr[] = {"key", "value"};
                            Log.e("Inside query reply ", array[0] + "$$$$$$$$$$$$$$$$$" + array[1]);
                            MatrixCursor matrixCursor = new MatrixCursor(arr);
                            String rows[] = {array_rep[0], array_rep[1]};
                            matrixCursor.addRow(rows);
                            flag_replication = false;
                            return matrixCursor;

                        }
                    }

                    }
            }
        }
    }


//    private class ClientTaskqueryall extends AsyncTask<String, Void, Void> {
//        @Override
//        protected Void doInBackground(String... msgs) {
//
//            String msgReceived = msgs[0];
//
//            node temp =chord_new.first;
//
//            for(int i=0 ; i < set.size() ; i++)
//            {
//                try {
//
//                    int port = Integer.parseInt(temp.node_val)*2;
//                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                            port);
//
//                    Log.e("forwarding query_all req",Integer.toString(port));
//                    //ekadanuncho + send all
//                    String msgToSend = getport()+"%"+msgReceived;
//                    Log.e("sending message to all",msgToSend);
//
//                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
//                    toServer.println(msgToSend);
//                    socket.close();
//                    temp=temp.succesor;
//
//                } catch (UnknownHostException e) {
//                    Log.e(TAG, "ClientTask UnknownHostException");
//                } catch (IOException e) {
//                    Log.e(TAG, "ClientTask socket IOException");
//                }
//            }
//
//            return null;
//
//        }
//    }

    private class ClientTaskquery extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String msgReceived = msgs[0];
            String msgparts[]=msgReceived.split("%");
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgparts[0]));
                String msgToSend = getport()+"%"+msgparts[1]+"%"+msgparts[2];
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgToSend);
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;

        }
    }

    private class ClientTaskqueryatrep extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String msgReceived = msgs[0];
            String msgparts[]=msgReceived.split("%");
            try {
                for (int i=0;i<2;i++)
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgparts[i])*2);
                    String msgToSend = getport()+"%"+msgparts[2]+"%"+"queryatrep";
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgToSend);
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;

        }
    }

    public MatrixCursor query_getall()
    {
        String arr[] = {"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(arr);
        try {
            Log.e("getall "," check this ");
//            for (int i = 0; i < seq; i++) {
//                String key_loop = hash.get(i);
//                String val = local_key_val.get(key_loop);
//                String rows[] = {key_loop, val};
//                Log.e("still has this value "+key_loop, val);
//                if(!val.equals(null))
//                {
//                    matrixCursor.addRow(rows);
//                }
//            }
//            matrixCursor.close();

            long start_test = System.currentTimeMillis();
            long end_test = System.currentTimeMillis();

            while(true)
            {
                end_test = System.currentTimeMillis();
                if(end_test-start_test >=1000)
                {
                    break;
                }
            }

            Iterator it = local_key_val.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                if (!pair.getValue().equals(null)) {
                    String rows[] = {pair.getKey().toString(),pair.getValue().toString()};
                    matrixCursor.addRow(rows);
                }
            }

            matrixCursor.close();

            return matrixCursor;
        }catch (Exception e)
        {

        }
        return matrixCursor;

    }


    public String query_local_string_all()
    {
        String tosendall=null;
        try {
            for (int i = 0; i < seq; i++) {
                String key_loop = hash.get(i);
                String val = local_key_val.get(key_loop);
                if(!val.equals(null))
                {
                    tosendall = tosendall +"%" +key_loop+"#"+val;
                }

            }

        }catch (Exception e)
        {
        }
        return tosendall;
    }


    public String storage_string_all()
    {
        String tosendall=null;
        try {
            Iterator it = query_storage.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                {
                    tosendall = tosendall + "%" + pair.getKey()+"#"+pair.getValue();
                }
            }



        }catch (Exception e)
        {
        }
        return tosendall;
    }


    public MatrixCursor query_local(String key)
    {
        MatrixCursor matrixCursor = null;
        String arr[] = {"key", "value"};

        matrixCursor = new MatrixCursor(arr);
        String val = local_key_val.get(key);
        String rows[] = {key, val};
        matrixCursor = new MatrixCursor(arr);
        matrixCursor.addRow(rows);
        matrixCursor.close();
        return matrixCursor;
    }


    public String query_local_string(String key)

    {
        String result=null;
        String val = local_key_val.get(key);
        result = key+"%"+val;
        Log.e("Qyery local string here",result);
        return result;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
