/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.testkit;


import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

public abstract class Client implements ExceptionListener
{
	private Connection con;
	private Session ssn;
    private boolean durable = false;
    private boolean transacted = false;
    private int txSize = 10;
    private int ack_mode = Session.AUTO_ACKNOWLEDGE;
    private String contentType = "application/octet-stream";

    private long reportFrequency = 60000;  // every min

    private DateFormat df = new SimpleDateFormat("yyyy.MM.dd 'at' HH:mm:ss");
    private NumberFormat nf = new DecimalFormat("##.00");

    private long startTime = System.currentTimeMillis();
    private ErrorHandler errorHandler = null;
    
    public Client(Connection con) throws Exception
    {
       this.con = con;  
       this.con.setExceptionListener(this);
       durable = Boolean.getBoolean("durable");
       transacted = Boolean.getBoolean("transacted");
       txSize = Integer.getInteger("tx_size",10);
       contentType = System.getProperty("content_type","application/octet-stream");    
       reportFrequency = Long.getLong("report_frequency", 60000);
    }

    public void close()
    {
    	try
    	{
    		con.close();
    	}
    	catch (Exception e)
    	{
    		handleError("Error closing connection",e);
    	}
    }
    
    public void onException(JMSException e)
    {
        handleError("Connection error",e);
    }
    
    public void setErrorHandler(ErrorHandler h)
    {
    	this.errorHandler = h;
    }
    
    public void handleError(String msg,Exception e)
    {
    	if (errorHandler != null)
    	{
    		errorHandler.handleError(msg, e);
    	}
    	else
    	{
    		System.err.println(msg);
    		e.printStackTrace();
    	}
    }

    protected Session getSsn()
    {
        return ssn;
    }

    protected void setSsn(Session ssn)
    {
        this.ssn = ssn;
    }

    protected boolean isDurable()
    {
        return durable;
    }

    protected boolean isTransacted()
    {
        return transacted;
    }

    protected int getTxSize()
    {
        return txSize;
    }

    protected int getAck_mode()
    {
        return ack_mode;
    }

    protected String getContentType()
    {
        return contentType;
    }

    protected long getReportFrequency()
    {
        return reportFrequency;
    }

    protected long getStartTime()
    {
        return startTime;
    }

    protected void setStartTime(long startTime)
    {
        this.startTime = startTime;
    }

    public DateFormat getDf()
    {
        return df;
    }
    
}
