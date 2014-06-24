package org.wso2.andes.server.store;


import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.exchange.Exchange;

public interface ExchangeManager {

    public void createExchange(Exchange exchange);

    public Exchange getExchange();

    public void addBinding(Exchange exchange, Binding binding);


}
