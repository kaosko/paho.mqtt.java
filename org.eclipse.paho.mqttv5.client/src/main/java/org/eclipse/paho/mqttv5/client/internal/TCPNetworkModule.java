/*******************************************************************************
 * Copyright (c) 2009, 2018 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0
 * and the Eclipse Distribution License is available at
 *   https://www.eclipse.org/org/documents/edl-v10.php
 *
 * Contributors:
 *    Dave Locke - initial API and implementation and/or initial documentation
 */
package org.eclipse.paho.mqttv5.client.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.eclipse.paho.mqttv5.client.MqttClientException;
import org.eclipse.paho.mqttv5.client.logging.Logger;
import org.eclipse.paho.mqttv5.client.logging.LoggerFactory;
import org.eclipse.paho.mqttv5.common.MqttException;

/**
 * A network module for connecting over TCP.
 */
public class TCPNetworkModule implements NetworkModule {
	private static final String CLASS_NAME = TCPNetworkModule.class.getName();
	private Logger log = LoggerFactory.getLogger(LoggerFactory.MQTT_CLIENT_MSG_CAT,CLASS_NAME);

	protected Socket socket;
	private SocketFactory factory;
	private String host;
	private int port;
	private int conTimeout;

	private String httpProxyHost;
	private int httpProxyPort;
	private String httpProxyUser;
	private String httpProxyPassword;

	/**
	 * Constructs a new TCPNetworkModule using the specified host and
	 * port.  The supplied SocketFactory is used to supply the network
	 * socket.
	 * @param factory the {@link SocketFactory} to be used to set up this connection
	 * @param host The server hostname
	 * @param port The server port
	 * @param resourceContext The Resource Context
	 */
	public TCPNetworkModule(SocketFactory factory, String host, int port, String resourceContext) {
		log.setResourceName(resourceContext);
		this.factory = factory;
		this.host = host;
		this.port = port;

	}

	/**
	 * Starts the module, by creating a TCP socket to the server.
	 * @throws IOException if there is an error creating the socket
	 * @throws MqttException if there is an error connecting to the server
	 */
	public void start() throws IOException, MqttException {
		final String methodName = "start";
		// @TRACE 252=connect to host {0} port {1} timeout {2}
		log.fine(CLASS_NAME, methodName, "252", new Object[] {host, Integer.valueOf(port), Long.valueOf(conTimeout*1000)});

		if (httpProxyHost != null) {
			Socket tunnel = null;
			try {
				tunnel = new Socket();
				tunnel.connect(new InetSocketAddress(httpProxyHost, httpProxyPort), conTimeout * 1000);
				// bound the proxy's reply time as well
				tunnel.setSoTimeout(Math.max(conTimeout, 1) * 1000);
				doTunnelHandshake(tunnel, host, port, httpProxyUser, httpProxyPassword);
			} catch (IOException ex) {
				//@TRACE 251=Failed to create TCP tunnel through HTTP proxy
				log.fine(CLASS_NAME, methodName, "251", null, ex);
				if (tunnel != null) {
					try {
						tunnel.close();
					} catch (IOException ignored) {
					}
				}
				throw new MqttException(MqttClientException.REASON_CODE_HTTP_PROXY_CONNECT_ERROR, ex);
			}

			try {
				if (factory instanceof SSLSocketFactory) {
					// layer TLS over the tunnel
					socket = ((SSLSocketFactory) factory).createSocket(tunnel, host, port, true);
				} else {
					socket = tunnel;
				}
				socket.setSoTimeout(1000);
			} catch (IOException ex) {
				//@TRACE 250=Failed to create TCP socket
				log.fine(CLASS_NAME, methodName, "250", null, ex);
				try {
					tunnel.close();
				} catch (IOException ignored) {
				}
				throw new MqttException(MqttClientException.REASON_CODE_SERVER_CONNECT_ERROR, ex);
			}
		} else {
			try {
				SocketAddress sockaddr = new InetSocketAddress(host, port);
				socket = factory.createSocket();
				socket.connect(sockaddr, conTimeout*1000);
				socket.setSoTimeout(1000);
			}
			catch (ConnectException ex) {
				//@TRACE 250=Failed to create TCP socket
				log.fine(CLASS_NAME,methodName,"250",null,ex);
				throw new MqttException(MqttClientException.REASON_CODE_SERVER_CONNECT_ERROR, ex);
			}
		}
	}

	/*
	 * Sends an HTTP CONNECT request to the proxy and checks for a 2xx reply.
	 * Throws IOException if anything goes wrong.
	 */
	private void doTunnelHandshake(Socket tunnel, String host, int port, String proxyUser, String proxyPassword)
			throws IOException {
		String target = host + ":" + port;
		StringBuilder msg = new StringBuilder();
		msg.append("CONNECT ").append(target).append(" HTTP/1.1\r\n");
		msg.append("Host: ").append(target).append("\r\n");
		msg.append("User-Agent: Paho MQTT5 Client\r\n");
		msg.append("Proxy-Connection: Keep-Alive\r\n");
		if (proxyUser != null && proxyPassword != null) {
			String credentials = proxyUser + ":" + proxyPassword;
			msg.append("Proxy-Authorization: Basic ")
					.append(java.util.Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)))
					.append("\r\n");
		}
		msg.append("\r\n");

		OutputStream out = tunnel.getOutputStream();
		out.write(msg.toString().getBytes(StandardCharsets.US_ASCII));
		out.flush();

		// Read the response headers byte by byte, so that nothing beyond the
		// blank line (e.g. TLS data from the broker) is consumed.
		InputStream in = tunnel.getInputStream();
		StringBuilder statusLine = new StringBuilder();
		boolean statusDone = false;
		int newlinesSeen = 0;
		int total = 0;
		while (newlinesSeen < 2) {
			int i = in.read();
			if (i < 0) {
				throw new IOException("Unexpected EOF from proxy");
			}
			if (++total > 16384) {
				throw new IOException("Proxy response headers too large");
			}
			if (i == '\n') {
				statusDone = true;
				++newlinesSeen;
			} else if (i != '\r') {
				newlinesSeen = 0;
				if (!statusDone && statusLine.length() < 200) {
					statusLine.append((char) i);
				}
			}
		}

		// Expect "HTTP/1.x 2xx ..."
		String[] parts = statusLine.toString().split(" ", 3);
		if (parts.length < 2 || !parts[0].startsWith("HTTP/") || !parts[1].startsWith("2")) {
			throw new IOException("Unable to tunnel through " + tunnel.getInetAddress().getHostName() + ":"
					+ tunnel.getPort() + ". Proxy returns \"" + statusLine + "\"");
		}
	}

	public InputStream getInputStream() throws IOException {
		return socket.getInputStream();
	}

	public OutputStream getOutputStream() throws IOException {
		return socket.getOutputStream();
	}

	/**
	 * Stops the module, by closing the TCP socket.
	 * @throws IOException if there is an error closing the socket
	 */
	public void stop() throws IOException {
		if (socket != null) {
			// CDA: an attempt is made to stop the receiver cleanly before closing the socket.
			// If the socket is forcibly closed too early, the blocking socket read in
			// the receiver thread throws a SocketException.
			// While this causes the receiver thread to exit, it also invalidates the
			// SSL session preventing to perform an accelerated SSL handshake in the
			// next connection.
			//
			// Also note that due to the blocking socket reads in the receiver thread,
			// it's not possible to interrupt the thread. Using non blocking reads in
			// combination with a socket timeout (see setSoTimeout()) would be a better approach.
			//
			// Please note that the Javadoc only says that an EOF is returned on
			// subsequent reads of the socket stream.
			// Anyway, at least with Oracle Java SE 7 on Linux systems, this causes a blocked read
			// to return EOF immediately.
			// This workaround should not cause any harm in general but you might
			// want to move it in SSLNetworkModule.

			socket.shutdownInput();
			socket.close();
		}
	}

	/**
	 * Set the maximum time to wait for a socket to be established
	 * @param timeout  The connection timeout
	 */
	public void setConnectTimeout(int timeout) {
		this.conTimeout = timeout;
	}

	public String getServerURI() {
		return "tcp://" + host + ":" + port;
	}

	/**
	 * Configures tunneling through an HTTP proxy. Ignored unless a host is given
	 * and the port is positive. Credentials are used only if both user and
	 * password are non-empty.
	 *
	 * @param proxyHost the proxy host
	 * @param proxyPort the proxy port
	 * @param user the proxy user, may be null
	 * @param password the proxy password, may be null
	 */
	public void configHttpProxy(String proxyHost, int proxyPort, String user, String password) {
		if (proxyHost != null && proxyHost.length() > 0 && proxyPort > 0) {
			this.httpProxyHost = proxyHost;
			this.httpProxyPort = proxyPort;
			if (user != null && user.length() > 0 && password != null && password.length() > 0) {
				this.httpProxyUser = user;
				this.httpProxyPassword = password;
			}
		}
	}
}
