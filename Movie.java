package com.prakash.spark.models;

import java.io.Serializable;

/**
 * Movie object to represent each line in csv file
 * https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset.
 */
public class Movie implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String color;
	private Integer numFaces;
	
	public Movie(String s, Integer i){
		this.color=s;
		this.numFaces=i;
	}
	
	public String getColor() {
		return color;
	}
	public void setColor(String color) {
		this.color = color;
	}
	public Integer getNumFaces() {
		return numFaces;
	}
	public void setNumFaces(Integer numFaces) {
		this.numFaces = numFaces;
	}
}
