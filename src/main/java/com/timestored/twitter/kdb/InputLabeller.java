package com.timestored.twitter.kdb;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;

import javax.swing.JLabel;
import javax.swing.JPanel;


/**
 * Standardises good practices of labelling and naming components.
 * Optionally displays help or making labels all fixed widths.
 */
public  class InputLabeller {
	
	private final Dimension labelDimension;
	
	public InputLabeller(int labelWidth, int labelHeight) {
		labelDimension = new Dimension(labelWidth, labelHeight);
	};

	public InputLabeller() {
		labelDimension = null;
	}

	public JPanel get(String labelText, Component inputComp, 
			String inputName) {
		
		return get(labelText, inputComp, inputName, null);
	}

	/**
	 * @param inputName The name set for the inputComponent useul for testing.
	 * @param helpComponent
	 * @return panel containing a left-hand label, for the given named input component
	 * and optionally to the right show a help component.
	 */
	public JPanel get(String labelText, Component inputComp, 
			String inputName, Component helpComponent) {
		
		JPanel p = new JPanel();
		FlowLayout flowLayout = (FlowLayout) p.getLayout();
		flowLayout.setAlignment(FlowLayout.LEFT);
		
		JLabel label = new JLabel(labelText);
		if(labelDimension!=null) {
			label.setPreferredSize(labelDimension);
		}
		label.setLabelFor(inputComp);
		inputComp.setName(inputName);
		
		p.add(label);
		p.add(inputComp);
		if(helpComponent != null) {
			p.add(helpComponent);
		}
		
		return p;
	}
}