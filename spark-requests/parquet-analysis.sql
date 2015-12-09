SELECT Order.ObName as OrderBook , SUM(order.Quantity) as TotalQuantity FROM records where type='Order' GROUP BY Order.ObName ORDER BY TotalQuantity DESC
