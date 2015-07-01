package com.timestored.twitter.kdb;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.imageio.ImageIO;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.border.TitledBorder;

import twitter4j.conf.Configuration;
import kx.c.KException;

public class KdbTwitFrame extends JFrame {

	public static final String HOST = "localhost";
	public static final int PORT = 5001;
	protected static final Color EDITED_COLOR = Color.YELLOW;
	
	private JSwitchBox streamButton;
	private final TwitterKdbModel twitterKdbModel;
	private JTextField streamCountField;
	private JTextField searchCountField;
	private JTextField tagsTextField;
	private JTextField woeidsTextField;
	private JSwitchBox searchButton;
	
	
	public KdbTwitFrame() {
		setTitle("Twitter Kdb Feedhandler");
		try {
			setIconImage(ImageIO.read(KdbTwitFrame.class.getResource("twitter.png")));
		} catch (IOException e1) {
			// ignore
		}
		// TODO take this in via GUI
		Configuration config = null;
		twitterKdbModel = new TwitterKdbModel(config, HOST, PORT);

		InputLabeller inputLabeller = new InputLabeller(100, 20);
		
		JPanel serverPanel = new JPanel(new GridLayout(1, 2));
		serverPanel.setBorder(new TitledBorder("Server"));
		JLabel serverLabel = new JLabel(HOST + ":" + PORT);
		serverPanel.add(inputLabeller.get("Host:", serverLabel, "serverLabel"));
		serverLabel.setBackground(JSwitchBox.GREEN);
		serverLabel.setOpaque(true);

		/*
		 * Create the filtered tag panel
		 */
		JPanel filteredPanel = new JPanel(new GridLayout(4, 1));
		
		JPanel p = new JPanel();
		searchButton = new JSwitchBox("On", "Off");
		searchButton.addActionListener(new ActionListener() {
			@Override public void actionPerformed(ActionEvent e) {
				twitterKdbModel.setSearch(!twitterKdbModel.isSearchOn());
				refreshUI();
			}
		});
		p.add(searchButton);
		searchCountField = getCounterField();
		p.add(inputLabeller.get("Tweets Sent:", searchCountField, "trendCountField"));
		filteredPanel.add(p);
		
		
		filteredPanel.setBorder(new TitledBorder("Filtered Stream"));
		tagsTextField = getTextField();
		tagsTextField.addActionListener(new ActionListener() {
			@Override public void actionPerformed(ActionEvent e) {
				List<String> queries;
				queries = Arrays.asList(tagsTextField.getText().split(","));
				twitterKdbModel.setSearch(queries);
			}
		});
		filteredPanel.add(inputLabeller.get("Tags:", tagsTextField, "tagsTextField"));

		woeidsTextField = getTextField();
		filteredPanel.add(inputLabeller.get("Locations:", woeidsTextField, "woeidsTextField"));
		p = new JPanel();
		p.add(new JButton("View Location List"));
		filteredPanel.add(p);

		JPanel randomPanel = createRandomStreamPanel(inputLabeller);
//		JPanel userPanel = createUserStreamPanel(inputLabeller);
		
		JPanel c = new JPanel();
		c.setLayout(new BoxLayout(c, BoxLayout.PAGE_AXIS));
		setLayout(new GridLayout(1, 1));
		c.add(serverPanel);
		c.add(filteredPanel);
		c.add(randomPanel);
//		c.add(userPanel);
		add(c);
		
		refreshUI();
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			  @Override public void run() {
			    EventQueue.invokeLater(new Runnable() {
					@Override public void run() {
						refreshUI();
					}
				});
			  }
			}, 1000, 1000);
	}

	private static JTextField getTextField() {
		final JTextField tf = new JTextField("");
		tf.setPreferredSize(new Dimension(280, 20));
		tf.addKeyListener(new KeyAdapter() {
			@Override public void keyReleased(KeyEvent e) {
				if(e.getKeyCode() != KeyEvent.VK_ENTER) {
					tf.setBackground(EDITED_COLOR);
				}
			}
		});
		tf.addActionListener(new ActionListener() {
			@Override public void actionPerformed(ActionEvent e) {
				tf.setBackground(Color.WHITE);
			}
		});
		return tf;
	}

	private JPanel createRandomStreamPanel(InputLabeller inputLabeller) {
		JPanel randomPanel = new JPanel();
		randomPanel.setBorder(new TitledBorder("Random Stream"));
		streamButton = new JSwitchBox("On", "Off");
		streamButton.addActionListener(new ActionListener() {
			@Override public void actionPerformed(ActionEvent e) {
				twitterKdbModel.setStream(!twitterKdbModel.isStreamOn());
				refreshUI();
			}
		});
		
		randomPanel.add(streamButton);
		streamCountField = getCounterField();
		randomPanel.add(inputLabeller.get("Tweets Sent:", streamCountField, "streamCountField"));
		return randomPanel;
	}
//
//	private static JPanel createUserStreamPanel(InputLabeller inputLabeller) {
//
//		
//		JSwitchBox sb = new JSwitchBox("On", "Off");
//
//		JPanel p = new JPanel();
//		p.add(sb);
//		JTextField cf = getCounterField();
//		cf.setText("0");
//		p.add(inputLabeller.get("Tweets Sent:", getCounterField(), "userCountField"));
//
//
//		JPanel c = new JPanel(new GridLayout(2, 1));
//		c.setBorder(new TitledBorder("User Stream"));
//		c.add(p);
//		c.add(inputLabeller.get("Users:", getTextField(), "userListField"));
//		
//		return c;
//	}
	
	public static JTextField getCounterField() {
		JTextField a = new JTextField("0");
		a.setPreferredSize(new Dimension(70, 20));
		a.setEditable(false);
		return a;
	}
	
	public void refreshUI() {
		streamButton.setSelected(twitterKdbModel.isStreamOn());
		streamCountField.setText(""+twitterKdbModel.getStreamCount());
		searchCountField.setText(""+twitterKdbModel.getSearchCount());
	}
	
	
	public static void main(String[] args) throws KException, IOException {
		SwingUtilities.invokeLater(new Runnable() {

			@Override
			public void run() {
				JFrame frame = new KdbTwitFrame();
				frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
//				frame.setPreferredSize(new Dimension(400, 200));
				frame.pack();
				frame.setVisible(true);
			}
		});
	}
}
