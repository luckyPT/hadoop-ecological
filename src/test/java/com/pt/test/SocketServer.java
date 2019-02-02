package com.pt.test;

import org.junit.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer {
    @Test
    public void startServer() throws IOException, InterruptedException {
        ServerSocket serverSocket = new ServerSocket(9999);
        Socket server = serverSocket.accept();

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(server.getOutputStream()));
        while (true) {
            bw.write("巴拉 巴拉 巴拉\n");
            bw.flush();
            Thread.sleep(1000);
        }
    }
}
