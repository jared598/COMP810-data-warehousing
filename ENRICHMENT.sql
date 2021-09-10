create or replace procedure enrichmentinlj as 
 
    data_size constant integer := 100;
   
    cursor v_cursor is
           select d.customer_id
                , d.datastream_id
                , d.warehouse_id
                , d.warehouse_name
                , d.customer_name  
                , d.t_date
                , d.quantity_sold
                , m.product_id
                , m.product_name
                , m.supplier_id
                , m.supplier_name
                , m.sale_price
             from datastream d
             join masterdata m on m.product_id = d.product_id;
             
    type t_cursor_records is table of v_cursor%rowtype;
    v_cursor_records t_cursor_records;
begin
    open v_cursor; 
    loop
        fetch v_cursor
         bulk collect
         into v_cursor_records
        limit data_size; 
        
        forall v_index in 1 .. v_cursor_records.count 
           insert into product(product_id, product_name, sale_price) 
           select v_cursor_records(v_index).product_id
                , v_cursor_records(v_index).product_name 
                , v_cursor_records(v_index).sale_price
             from dual
            where not exists 
                 ( select null 
                     from product
                    where product_id = v_cursor_records(v_index).product_id
                  );
                  
        forall v_index in 1 .. v_cursor_records.count 
           insert into supplier(supplier_id, supplier_name)
           select v_cursor_records(v_index).supplier_id
                 , v_cursor_records(v_index).supplier_name
              from dual
            where not exists 
                 ( select null 
                     from supplier
                    where supplier_id = v_cursor_records(v_index).supplier_id
                  ); 
        
        forall v_index in 1 .. v_cursor_records.count
           insert into customer(customer_id, customer_name)
           select v_cursor_records(v_index).customer_id
                   , v_cursor_records(v_index).customer_name
                from dual
            where not exists 
                 ( select null 
                     from customer
                    where customer_id = v_cursor_records(v_index).customer_id
                  );  
                  
        forall v_index in 1 .. v_cursor_records.count
           insert into warehouse(warehouse_id, warehouse_name)
              select v_cursor_records(v_index).warehouse_id
                   , v_cursor_records(v_index).warehouse_name 
                from dual
            where not exists 
                 ( select null 
                     from warehouse
                    where warehouse_id = v_cursor_records(v_index).warehouse_id
                  ); 
                  
        forall v_index in 1 .. v_cursor_records.count
           insert into date_time(date_id, t_date, time_year, time_month, time_day)
              select v_cursor_records(v_index).datastream_id
                   , v_cursor_records(v_index).t_date
                   , extract(year from v_cursor_records(v_index).t_date)
                   , extract(month from v_cursor_records(v_index).t_date)
                   , extract(day from v_cursor_records(v_index).t_date)
                from dual
            where not exists 
                 ( select null 
                     from date_time
                    where date_id = v_cursor_records(v_index).datastream_id
                  );                  
 
        forall v_index in 1 .. v_cursor_records.count
           insert into sales( customer_id
                                 , product_id
                                 , warehouse_id
                                 , supplier_id
                                 , date_id
                                 , total_sale
                                 , quantity_sold
                                 )
              select v_cursor_records(v_index).customer_id
                   , v_cursor_records(v_index).product_id
                   , v_cursor_records(v_index).warehouse_id
                   , v_cursor_records(v_index).supplier_id 
                   , v_cursor_records(v_index).datastream_id
                   , v_cursor_records(v_index).quantity_sold
                     * v_cursor_records(v_index).sale_price
                   , v_cursor_records(v_index).quantity_sold
               from dual
              where not exists 
                  ( select null 
                      from sales
                     where product_id = v_cursor_records(v_index).product_id
                       and customer_id = v_cursor_records(v_index).customer_id
                       and supplier_id = v_cursor_records(v_index).supplier_id
                       and warehouse_id = v_cursor_records(v_index).warehouse_id
                       and date_id = v_cursor_records(v_index).datastream_id
                       and total_sale = v_cursor_records(v_index).sale_price * v_cursor_records(v_index).quantity_sold
                  );
                  
       exit when v_cursor_records.count < data_size;              

    end loop; 
 
    close v_cursor;    
    commit;
    
end  enrichmentinlj; 