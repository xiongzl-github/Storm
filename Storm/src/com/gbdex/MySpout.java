package com.gbdex;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MySpout implements IRichSpout{
	private static final long serialVersionUID = 1L;
	FileInputStream fis;
	InputStreamReader isr;
	BufferedReader br;			
	SpoutOutputCollector collector = null;
	String str = null;

	@Override
	public void nextTuple() {
		try {
			while ((str = this.br.readLine()) != null) {
				// 过滤动作
				collector.emit(new Values(str));
				//Thread.sleep(2000);
				//to do 
				//System.err.println("MySpout(nextTuple)-------------------------------->");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		//System.err.println("MySpout(open)------------------->");
		try {
			this.collector = collector;
			this.fis = new FileInputStream("track.log");
			this.isr = new InputStreamReader(fis, "UTF-8");
			this.br = new BufferedReader(isr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));
		//System.err.println("MySpout(declareOutputFields)------------------------>");
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		//ystem.err.println("MySpout(getComponentConfiguration)----------------------->");
		return null;
	}
	@Override
	public void ack(Object msgId) {
		//System.err.println("MySpout(ack)---------------------------->");
	}

	@Override
	public void activate() {
		//System.err.println("mySpout(active)------------------------------->");
	}

	@Override
	public void close() {
		//System.err.println("mySpout(close)------------------------------->");
	}

	@Override
	public void deactivate() {
		//System.err.println("mySpout(deactivate)------------------------------->");
	}

	@Override
	public void fail(Object msgId) {
		//System.err.println("mySpout(fail)------------------------------->");
	}

}
