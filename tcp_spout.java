/*******************************************************************************
 * Copyright (c) 2014 Valerio Formicola.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *  Valerio Formicola, 
 *  PhD in Information Engineering
 *  https://github.com/ValeFormi
 ******************************************************************************/




import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class tcp_spout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	DataInputStream in;

	private boolean flag = false;
	private static int port = 30247, maxConnections = 0;

	public tcp_spout(int set_port) {
		super();
		flag = false;
		if(set_port!=0) port = set_port;
	}

	public boolean isFlag() {
		return flag;
	}

	public void setFlag(boolean flag) {
		this.flag = flag;
	}

	class doComms implements Runnable {

		private tcp_spout ref_spout;
		private Socket socket;

		public doComms(Socket socket, tcp_spout ref_spout) {
			super();
			this.ref_spout = ref_spout;
			this.socket = socket;
		}

		@Override
		public void run() {

			String line = "";

			try {
				// Get input from the client
				// DataInputStream in = new DataInputStream
				// (server.getInputStream());
				// PrintStream out = new PrintStream(server.getOutputStream());

				while ((line = in.readLine()) != null) {

					System.out.println("Event: " + line);

					

					if (line.length() > 0) {
						Values vals = new Values(line);
						_collector.emit(vals);
					}
					line = "";
				}

			
				socket.close();
				ref_spout.setFlag(false);

			} catch (IOException ioe) {

				System.out.println("IOException on socket listen: " + ioe);
				ref_spout.setFlag(false);

				try {
					// in.close();
					socket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		int i = 0;
		// if the server is not started, start it
		if (flag == false) {
			// and lock the starting process for nextTuple call
			flag = true;
			try {
				ServerSocket listener = new ServerSocket(port);
				Socket server;
				while ((i++ < maxConnections) || (maxConnections == 0)) {
					server = listener.accept();
					doComms conn_c = new doComms(server, this);
					Thread t = new Thread(conn_c);
					in = new DataInputStream(server.getInputStream());
					t.start();
				}
			} catch (IOException ioe) {
				System.out.println("IOException on socket listen: " + ioe);
				ioe.printStackTrace();
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("event"));
	}

}
