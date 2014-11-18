CREATE OR REPLACE PROCEDURE APPS.XXRLJE_PROCESS_SPHE_E1_TRX(ERRBUFF Varchar2,ERRCODE Varchar2) as

--we will run this after the datamart
--which will set all the E1 transactions to N
     cursor e1_hdr_cur is
         select distinct invoice_cm_number,INVOICE_CM_TYPE_DESCRIPTION
         from   XXRLJ.xxsphe_transactions
         where  process_flag = 'I'
         and    trim(customer_number) ='000000000000222995.'
         --and    invoice_cm_number = '53455216'
         ;
         
     cursor e1_dtl_cur(p_inv_num varchar2,p_inv_cm_type varchar2) is
         select *
         from   XXRLJ.xxsphe_transactions
         where  process_flag = 'I'
         and    trim(customer_number) = '000000000000222995.'
         and    invoice_cm_number = p_inv_num
         and    INVOICE_CM_TYPE_DESCRIPTION = p_inv_cm_type;
         
     cursor dupl_inv_fix_cur is 
         select count(*) trx_count,trx_number
         from (select distinct interface_line_attribute1,trx_number
               from   ra_interface_lines_all
               where  interface_line_context = 'E1 TRANSACTIONS')
         group by trx_number         
         having count(*) > 1    ;
         
     cursor int_line_cur(p_trx_number varchar2) is
         select distinct interface_line_attribute1
         from   ra_interface_lines_all
         where  interface_line_context = 'E1 TRANSACTIONS'
         and    trx_number = p_trx_number; 
         
           
         
         vn_item_id          number;
         vc_item             varchar2(100);
         vc_err_msg          varchar2(1000);
         vc_process_flag     varchar2(10);
         vc_trx_type         varchar2(100);
         vc_order_type       varchar2(100);
         vc_po_number        varchar2(100);
         vn_price            number;
         vn_trx_number       number;
         vn_new_trx_number   number;   
         i                   number;
         vn_cust_account_id  number;
         vn_cust_acct_site_id number;
         vn_salesrep_id      number;
         vn_term_id          number;
         vn_cust_trx_type_id number;
         vc_cm_inv           varchar2(100);
         vc_segment1         varchar2(100);
         vc_segment2         varchar2(100);
         vc_segment3         varchar2(100);
         vc_segment4         varchar2(100);
         vc_segment5         varchar2(100);
         vc_segment6         varchar2(100);
         vc_segment7         varchar2(100);
         vc_segment8         varchar2(100);
         vc_segment9         varchar2(100);
         vc_segment10        varchar2(100);
         vc_description      varchar2(1000);
         vc_rights           varchar2(100);
         vc_customer_number  varchar2(100);


BEGIN

   update XXRLJ.xxsphe_transactions
   set process_flag = 'Y'
   where trim(customer_number) ='000000000000222995.'
   and quantity = 0
   and process_flag = 'I'; 
   commit;

   for e1_hdr_rec in e1_hdr_cur loop
        vc_process_flag := 'Y';
        vc_err_msg    := NULL;
   
        BEGIN
            select datamart_transaction_type,datamart_order_type
            into   vc_trx_type,vc_order_type
            from   XXRLJ.xxsphe_sphe_to_datamart_xref
            where  sphe_invoice_cm_type = e1_hdr_rec.invoice_cm_type_description;
        EXCEPTION
            WHEN OTHERS THEN
                vc_err_msg := vc_err_msg ||'-Unmapped SPHE Trx Type'; 
                vc_process_flag  := 'E';   
        END; 
        
        BEGIN
            select gcc.segment1
                  ,gcc.segment2
                  ,gcc.segment8
                  ,gcc.segment10
                  ,RCTT.TYPE
                  ,RCTT.CUST_TRX_TYPE_ID
            into   vc_segment1
                  ,vc_segment2 
                  ,vc_segment8
                  ,vc_segment10
                  ,vc_cm_inv            
                  ,vn_cust_trx_type_id         
            from   ra_cust_trx_types_all rctt
                 ,gl_code_combinations gcc
            where  upper(rctt.name) =  upper(vc_trx_type)
            and    rctt.gl_id_rev   = gcc.code_combination_id;
        
        
        EXCEPTION
            WHEN OTHERS THEN
               vc_err_msg := vc_err_msg ||'-Invalid AR Trx Type'; 
               vc_process_flag  := 'E';   
        END;
                
        
        BEGIN
            select hca.cust_account_id, hcas.cust_acct_site_id, hcsu.primary_salesrep_id, term.term_id
            into   vn_cust_account_id,vn_cust_acct_site_id,vn_salesrep_id,vn_term_id
            from   ra_terms               term
                  ,hz_customer_profiles   cp
                  ,hz_parties             hzp
                  ,hz_cust_accounts       hca
                  ,hz_cust_acct_sites_all hcas
                  ,hz_cust_site_uses_all  hcsu
            where hca.cust_account_id     = hcas.cust_account_id
            and   hca.cust_account_id     = cp.cust_account_id
            and   cp.standard_terms       = term.term_id(+)
            and   hcas.cust_acct_site_id  = hcsu.cust_acct_site_id
            and   hcsu.site_use_code      = 'BILL_TO'
            and   hca.party_id            = hzp.party_id
            and   hca.account_number      = '97130'
            and   rownum                  = 1;
        
        EXCEPTION
            WHEN OTHERS THEN 
               vc_err_msg := vc_err_msg ||'-Error in Getting Customer Details'; 
               vc_process_flag  := 'E';           
        END;
        
        i := 0;
        
        IF vc_process_flag <> 'E' THEN
            select XXRLJ.xxrlje_e1_transactions_seq.nextval
            into   vn_trx_number 
            from   dual;
            vc_customer_number := '97130';
    
            for e1_dtl_rec in e1_dtl_cur(e1_hdr_rec.invoice_cm_number,e1_hdr_rec.INVOICE_CM_TYPE_DESCRIPTION) loop
                
                vc_process_flag := 'Y';
                vc_err_msg := NULL;
                i := i + 1;
                                
                select decode(vc_cm_inv,'CM',NULL,vn_term_id)
                into   vn_term_id
                from   dual;
                
                BEGIN
                      select inventory_item_id,segment1,description
                      into   vn_item_id,vc_item,vc_description
                      from   mtl_system_items_b
                      where  replace(segment1,'-','') = decode(substr(trim(e1_dtl_rec.item_number),-3),'LIT',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                                                                       'WMU',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                                                                       'BST',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                                                                       'CC1',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                                                                       'KM1',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                                                                       'KM3',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                                                                       'KM4',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                                                                       'KM5',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                                                                       'HQC',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),                                                                                                        
                                                                                                       'SM1',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                                                                       'BJS',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                                                                       'HPR',substr(trim(e1_dtl_rec.item_number),1,length(trim(e1_dtl_rec.item_number))-3),
                                                       decode(substr(trim(e1_dtl_rec.item_number),1,2),'MD',substr(trim(e1_dtl_rec.item_number),2,length(trim(e1_dtl_rec.item_number))),trim(e1_dtl_rec.item_number))
                                                       )
                      and    organization_id = 85
                      --and    inventory_item_status_code = 'Active'
                      ;
                      
                      --dbms_output.put_line('Item '||vc_item);
                EXCEPTION
                     WHEN OTHERS THEN
                         vc_err_msg := 'Invalid Item';
                         vc_process_flag  := 'E';                   
                END;  
                
                IF vn_item_id is not null THEN
                    BEGIN                       
                        select gcc.segment3
                              ,gcc.segment4
                              ,gcc.segment5
                              ,gcc.segment6
                              ,gcc.segment7
                              ,gcc.segment8
                              ,gcc.segment10
                        into   vc_segment3
                              ,vc_segment4
                              ,vc_segment5
                              ,vc_segment6
                              ,vc_segment7  
                              ,vc_segment8  
                              ,vc_segment10              
                        from   mtl_system_items_b   msi
                              ,gl_code_combinations gcc
                        where  msi.inventory_item_id     = vn_item_id
                        and    msi.organization_id       = 85
                        and    msi.cost_of_sales_account = gcc.code_combination_id;
                        --dbms_output.put_line('Segment3'||vc_segment3);
                        
                        FND_FILE.PUT_LINE(FND_FILE.OUTPUT,vc_item||'-'||vc_segment9);
                        
                        BEGIN
                            select worldwide_rights
                            into   vc_rights 
                            from   xxrlje_item_extension_legal
                            where  inventory_item_id = vn_item_id;
                        EXCEPTION
                            WHEN OTHERS THEN
                               NULL;
                               vc_rights := '1';
                        END;
                                   
                        IF vc_segment10 = '21' THEN
                            IF vc_cm_inv = 'INV' THEN
                               vn_cust_trx_type_id := '1150';
                            ELSE
                               vn_cust_trx_type_id := '1122';   
                            END IF;   
                        ELSIF vc_segment10 = '30' THEN
                            IF vc_cm_inv = 'INV' THEN
                               vn_cust_trx_type_id := '1145';
                            ELSE
                               vn_cust_trx_type_id := '1014';   
                            END IF;          
                        ELSE
                            IF vc_cm_inv = 'INV' THEN
                               vn_cust_trx_type_id := '1143';
                            ELSE
                               vn_cust_trx_type_id := '1013';   
                            END IF;                                                
                        END IF;
                        
                        
                        
                        /*
                        FND_FILE.PUT_LINE(FND_FILE.LOG,vc_segment5||'-'||vc_rights);
                        
                        
                        IF vc_segment5 = '1210' and vc_rights = '550' THEN --for us only/ BD it should be a diff cust and rev share trx type
                        
                            BEGIN
                                select hca.cust_account_id, hcas.cust_acct_site_id, hcsu.primary_salesrep_id, term.term_id
                                into   vn_cust_account_id,vn_cust_acct_site_id,vn_salesrep_id,vn_term_id
                                from   ra_terms               term
                                      ,hz_customer_profiles   cp
                                      ,hz_parties             hzp
                                      ,hz_cust_accounts       hca
                                      ,hz_cust_acct_sites_all hcas
                                      ,hz_cust_site_uses_all  hcsu
                                where hca.cust_account_id     = hcas.cust_account_id
                                and   hca.cust_account_id     = cp.cust_account_id
                                and   cp.standard_terms       = term.term_id(+)
                                and   hcas.cust_acct_site_id  = hcsu.cust_acct_site_id
                                and   hcsu.site_use_code      = 'BILL_TO'
                                and   hca.party_id            = hzp.party_id
                                and   hca.account_number      = '95463'
                                and   rownum                  = 1;
                                
                                vc_customer_number := '95463';
                                
                                IF vc_cm_inv = 'INV' THEN
                                   vn_cust_trx_type_id := '1146';
                                ELSE
                                   vn_cust_trx_type_id := '1218';   
                                END IF;
                            EXCEPTION
                                WHEN OTHERS THEN 
                                   vc_err_msg := vc_err_msg ||'-Error in Getting Customer Details'; 
                                   vc_process_flag  := 'E';           
                            END;
                        END IF;    
                     */   
                    EXCEPTION    
                        WHEN OTHERS THEN
                           vc_err_msg := 'Error in getting Accounting Segments From Item';
                           vc_process_flag  := 'E';                   
                    END; 
                END IF;
                
                                                     
                vc_po_number := e1_dtl_rec.cust_po_number; -- 05/25/2011 PN added for the cust_po_number field added in the feed
                
                BEGIN
                    select gcc.segment1
                          ,gcc.segment2
                          ,gcc.segment9
                          ,RCTT.TYPE
                          ,RCTT.CUST_TRX_TYPE_ID
                    into   vc_segment1
                          ,vc_segment2 
                          ,vc_segment9
                          ,vc_cm_inv            
                          ,vn_cust_trx_type_id         
                    from   ra_cust_trx_types_all rctt
                          ,gl_code_combinations gcc
                    where  rctt.gl_id_rev   = gcc.code_combination_id
                    and    rctt.cust_trx_type_id =  vn_cust_trx_type_id;   
                EXCEPTION
                    WHEN OTHERS THEN
                       vc_err_msg := 'Error in validating Trx Type';
                       vc_process_flag  := 'E';
                END;      
                
                IF vc_process_flag <> 'E' THEN 
                   vn_price := XXRLJE_GET_SELLING_PRICE(vc_customer_number,vc_item);
                   --XXIMG_GET_SELLING_PRICE@PROD11i.rljentertainment.com(vc_customer_number,vc_item,vc_order_type,NULL); 
                   --dbms_output.put_line('Price '||vn_price);
                   IF vn_price is not null THEN
                       BEGIN
                           INSERT INTO ra_interface_lines_all(
                                         batch_source_name
                                        ,interface_line_context
                                        ,interface_line_attribute1
                                        ,interface_line_attribute2
                                        ,interface_line_attribute3
                                        ,sales_order
                                        ,sales_order_line
                                        ,sales_order_date
                                        ,orig_system_sold_customer_id
                                        ,orig_system_bill_customer_id
                                        ,orig_system_bill_address_id
                                        ,orig_system_ship_customer_id
                                        ,orig_system_ship_address_id                                
                                        ,amount
                                        ,cust_trx_type_id
                                        ,term_id
                                        ,trx_number
                                        ,line_number
                                        ,line_type
                                        ,inventory_item_id
                                        ,description
                                        ,uom_code
                                        ,trx_date
                                        ,gl_date
                                        ,purchase_order
                                        ,quantity
                                        ,quantity_ordered
                                        ,unit_standard_price
                                        ,unit_selling_price
                                        ,primary_salesrep_id
                                        ,comments
                                        ,set_of_books_id
                                        ,org_id
                                        ,tax_code
                                        ,currency_code
                                        ,created_by
                                        ,creation_date
                                        ,last_updated_by
                                        ,last_update_date
                                        ,conversion_type
                                        ,conversion_rate
                                        ,printing_option
                                        )
                                     VALUES ('E1 Transactions'                 -- batch_source_name **** change
                                            ,'E1 TRANSACTIONS'                 -- interface_line_context
                                            ,vn_trx_number||'-'||vn_cust_trx_type_id                     -- interface_line_attribute1
                                            ,e1_hdr_rec.invoice_cm_number      -- interface_line_attribute2
                                            ,i                                 -- interface_line_attribute3
                                            ,NULL                              -- sales_order
                                            ,NULL                              -- sales_order_line
                                            ,NULL                              -- sales_order_date
                                            ,vn_cust_account_id                -- orig_system_sold_customer_id
                                            ,vn_cust_account_id                -- orig_system_bill_customer_id
                                            ,vn_cust_acct_site_id              -- orig_system_bill_address_id
                                            ,vn_cust_account_id                -- orig_system_ship_customer_id
                                            ,vn_cust_acct_site_id              -- orig_system_ship_address_id                                    
                                            ,e1_dtl_rec.quantity*vn_price      -- amount
                                            ,vn_cust_trx_type_id               -- v_cust_trx_type_id
                                            ,vn_term_id                        -- term_id
                                            ,vn_trx_number                     -- trx_number
                                            ,i                                 -- line_number
                                            ,'LINE'                            -- line_type
                                            ,vn_item_id                        -- inventory_item_id
                                            ,vc_description                    -- description
                                            ,'EA'                              -- uom_code
                                            ,e1_dtl_rec.invoice_cm_date        -- trx_date
                                            ,e1_dtl_rec.invoice_cm_date        -- gl_date
                                            ,vc_po_number                      -- purchase_order
                                            ,e1_dtl_rec.quantity               -- quantity
                                            ,NULL                              -- quantity_ordered
                                            ,vn_price                          -- unit_standard_price
                                            ,vn_price                          -- unit_selling_price
                                            ,vn_salesrep_id                    -- primary_salesrep_id
                                            ,NULL                              -- comments
                                            ,2021                                 -- set_of_books_id
                                            ,84
                                            ,NULL
                                            ,'USD'                                   
                                            ,3
                                            ,SYSDATE
                                            ,3
                                            ,SYSDATE
                                            ,'User'
                                            ,1
                                            ,'PRI'
                                            )
                                            ;
                       EXCEPTION
                           WHEN OTHERS THEN
                              vc_process_flag := 'E';
                              vc_err_msg      := vc_err_msg||'-'||SQLCODE||'-'||SQLERRM||'-'||'Error in Line Insert';                     
                       END;
                       
                       BEGIN
                           INSERT INTO ra_interface_distributions_all
                                           (interface_line_context
                                           ,interface_line_attribute1
                                           ,interface_line_attribute2
                                           ,interface_line_attribute3
                                           ,account_class
                                           ,amount
                                           ,PERCENT
                                           ,segment1
                                           ,segment2
                                           ,segment3
                                           ,segment4
                                           ,segment5
                                           ,segment6
                                           ,segment7
                                           ,segment8
                                           ,segment9
                                           ,segment10
                                           ,org_id
                                          )
                                    VALUES('E1 TRANSACTIONS'
                                          ,vn_trx_number||'-'||vn_cust_trx_type_id                     -- interface_line_attribute1
                                          ,e1_hdr_rec.invoice_cm_number                                -- interface_line_attribute2
                                          ,i                                                           -- interface_line_attribute3
                                          ,'REV'
                                          ,e1_dtl_rec.quantity*vn_price
                                          ,100
                                          ,vc_segment1
                                          ,vc_segment2
                                          ,vc_segment3
                                          ,vc_segment4
                                          ,vc_segment5
                                          ,vc_segment6
                                          ,vc_segment7
                                          ,vc_segment8
                                          ,vc_segment9
                                          ,vc_segment10
                                          ,84
                                          )
                                          ;        
                       EXCEPTION
                           WHEN OTHERS THEN
                              vc_process_flag := 'E';
                              vc_err_msg      := vc_err_msg||'-'||SQLCODE||'-'||SQLERRM||'-'||'Error in Distribution Insert';                     
                       END;
                   ELSE
                       vc_process_flag := 'E';
                       vc_err_msg      := vc_err_msg||'-'||'Price Not set';     
                       
                   END IF;    
                END IF;
                --change
                update XXRLJ.xxsphe_transactions
                set    process_flag = vc_process_flag,error_msg = vc_err_msg
                where  sequence_no = e1_dtl_rec.sequence_no;
             
            end loop;            
            
        END IF;
        
        
        
        update XXRLJ.xxsphe_transactions
        set    process_flag = vc_process_flag,error_msg = vc_err_msg
        where  trim(customer_number) = '000000000000222995.'
        and    process_flag = 'I'
        and    invoice_cm_number = e1_hdr_rec.invoice_cm_number
        and    INVOICE_CM_TYPE_DESCRIPTION = e1_hdr_rec.invoice_cm_type_description; 
   END LOOP;   
   
   for dupl_inv_fix_rec in dupl_inv_fix_cur loop --for the madacy duplicate invoice
       for int_line_rec in int_line_cur(dupl_inv_fix_rec.trx_number) loop
           select xxrlj.xxrlje_e1_transactions_seq.nextval
           into   vn_new_trx_number
           from   dual;
               
           update ra_interface_lines_all
           set    trx_number = vn_new_trx_number
           where  interface_line_context = 'E1 TRANSACTIONS'
           and    interface_line_attribute1 = int_line_rec.interface_line_attribute1;
               
       end loop;
   end loop;
   
      
END;
/
