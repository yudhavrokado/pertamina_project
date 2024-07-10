BEGIN
  FOR cur_date IN (SELECT TO_DATE('2022-01-01', 'YYYY-MM-DD') + LEVEL - 1 AS current_date
                   FROM dual
                   CONNECT BY LEVEL <= TO_DATE('2024-04-23', 'YYYY-MM-DD') - TO_DATE('2022-01-01', 'YYYY-MM-DD') + 1)
  LOOP
	--PICCST022
  	INSERT INTO APP_IEM_INOVASI.NEW_VALUE_CREATION
  	SELECT
	  'INOVASI' USE_CASE,
	  'SPBU' SUB_USE_CASE,
	  'PICCST022' EXCEPT_CODE,
	  'Transaksi Solar tidak wajar > 200 liter' EXCEPT_NAME,
	  'NOMOR SPBU' "OBJECT",
	  z.REGIONAL,
	  z.SBM,
	  z.SAM,
	  TRUNC(max(b.completed_ts)) as date_ts,
	  max(grade_name_std) as grade_name_std,
	  sum(b.delivery_volume) as delivery_volume,
	  sum(b.delivery_value) as delivery_value,
	  count(DISTINCT b.site_id) as jumlah
	from
	  iem_vw_iedcc_det_catatan_transaksi b
	  left join (
	    select distinct
	      nopol
	    from
	      iem_tbl_spbu_nopol_pengecualian
	    where
	      trunc(ticket_Date) >= trunc(cur_date.current_date) ---30 jika sysdate dilepas
	  ) n on trim(b.vehicle_number) = trim(n.nopol)
	  left join (
	    select distinct
	      nopol
	    from
	      iem_tbl_spbu_signal_action
	    where
	      response in ('Recomendasi', 'Transaksi Wajar')
	      and trunc(cur_date.current_date) between trunc(start_date) and trunc(end_date)
	  ) rr on trim(b.vehicle_number) = trim(rr.nopol)
	  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id
	where
	  trunc (b.completed_ts) = trunc(cur_date.current_date) ---1 jika sysdate dilepas
	  and b.grade_name_std = 'BIO_SOLAR'
	  and b.delivery_volume >= 201
	  and b.delivery_value > 0
	  and (
	    b.VEHICLE_TYPE = 5
	    or b.VEHICLE_TYPE is null
	  )
	  and (
	    length (b.agency_name) < 10
	    or b.agency_name is null
	  )
	  and n.nopol is null
	  and rr.nopol is null
	group by
	  z.REGIONAL,
	  z.SBM,
	  z.SAM;
	 COMMIT;
	
	--PICCST023
	INSERT INTO APP_IEM_INOVASI.NEW_VALUE_CREATION
	select 
	  'INOVASI' USE_CASE, 
	  'SPBU' SUB_USE_CASE, 
	  'PICCST023' EXCEPT_CODE, 
	  'Transaksi Solar berulang pada no polisi sama di hari yang sama > 200 liter' EXCEPT_NAME, 
	  'NOMOR POLISI' "OBJECT", 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM, 
	  TRUNC(
	    max(b.completed_ts)
	  ) as date_ts, 
	  max(grade_name_std) as grade_name_std, 
	  sum(b.delivery_volume) as delivery_volume, 
	  sum(b.delivery_value) as delivery_value, 
	  count(DISTINCT b.vehicle_number) as jumlah 
	from 
	  (
	    select 
	      b.* 
	    from 
	      (
	        select 
	          b.*
	        from 
	          (
	            select 
	              b.*, 
	              dense_rank() over(
	                partition by vehicle_number 
	                order by 
	                  completed_ts desc
	              ) as drank, 
	              sum(jlh) over(
	                partition by trim(b.VEHICLE_NUMBER)
	              ) as jumlah, 
	              sum(delivery_volume) over(
	                partition by trim(b.VEHICLE_NUMBER)
	              ) as delivery_volume2, 
	              sum(delivery_value) over(
	                partition by trim(b.VEHICLE_NUMBER)
	              ) as delivery_value2 
	            from 
	              (
	                select 
	                  trunc(b.completed_ts) as date_ts, 
	                  min(b.completed_ts) as min_completed_ts, 
	                  max(b.completed_ts) as completed_ts, 
	                  trim(b.vehicle_number) as vehicle_number, 
	                  b.site_id, 
	                  b.grade_name_std, 
	                  sum(b.delivery_volume) as delivery_volume, 
	                  sum(b.delivery_value) as delivery_value, 
	                  count(*) as jlh 
	                from 
	                  iem_vw_iedcc_det_catatan_transaksi b 
	                  left join (
	                    select 
	                      distinct nopol 
	                    from 
	                      iem_tbl_spbu_nopol_pengecualian 
	                    where 
	                      trunc(ticket_Date) >= trunc(cur_date.current_date)---30
	                  ) n on trim(b.vehicle_number)= trim(n.nopol) 
	                  left join (
	                    select 
	                      distinct nopol 
	                    from 
	                      iem_tbl_spbu_signal_action 
	                    where 
	                      response in (
	                        'Recomendasi', 'Transaksi Wajar'
	                      ) 
	                      and trunc(cur_date.current_date) between trunc(start_date) 
	                      and trunc(end_date)
	                  ) rr on trim(b.vehicle_number)= trim(rr.nopol) 
	                where 
	                  trunc(b.completed_ts) = trunc(cur_date.current_date) ---1 
	                  and n.nopol is null 
	                  and rr.nopol is null 
	                  and b.grade_name_std = 'BIO_SOLAR' 
	                  and trim(b.vehicle_number) not like 'QR%DUA' 
	                  and trim(b.vehicle_number) not like 'RO%DUA' 
	                  and trim(b.vehicle_number) not like 'QR%DA' 
	                  and trim(b.vehicle_number) not like 'TERA%' 
	                  and trim(b.vehicle_number) not like 'T3R4%' 
	                  and trim(b.vehicle_number) not like 'T3R4%' 
	                  and trim(b.vehicle_number) not like 'T3RA%' 
	                  and trim(b.vehicle_number) not like 'TER4%' 
	                  and trim(b.vehicle_number) not like 'TB%RAB' 
	                  and trim(b.vehicle_number) not like 'TE%RAB' 
	                  and trim(b.vehicle_number) not like 'TM%RAB' 
	                  and trim(b.vehicle_number) not like 'TQ%RAB' 
	                  and trim(b.vehicle_number) not like 'TR%DA' 
	                  and trim(b.vehicle_number) not like 'TR%DUA' 
	                  and trim(b.vehicle_number) not like 'TR%RAB' 
	                  and trim(b.vehicle_number) not like 'TX%RAB' 
	                  and trim(b.vehicle_number) not like 'TY%RAB' 
	                  and trim(b.vehicle_number) not like 'TA%RAP' 
	                  and trim(b.vehicle_number) not like 'TE%RAP' 
	                  and trim(b.vehicle_number) not like 'TM%RAP' 
	                  and trim(b.vehicle_number) not like 'TY%RAB' 
	                  and trim(b.vehicle_number) not like 'TA%RAP' 
	                  and trim(b.vehicle_number) not like 'TP%RAP' 
	                  and trim(b.vehicle_number) not like 'TP%RAP' 
	                  and trim(b.vehicle_number) not like 'TR%PR' 
	                  and trim(b.vehicle_number) not like 'TR%PER' 
	                  and trim(b.vehicle_number) not like 'TR%RAP' 
	                  and trim(b.vehicle_number) not like 'TW%RAP' 
	                  and trim(b.vehicle_number) not like 'PS%SPB' 
	                  and trim(b.vehicle_number) not like 'PE%SPP' 
	                group by 
	                  trunc(b.completed_ts), 
	                  trim(b.vehicle_number), 
	                  b.grade_name_std, 
	                  b.site_id
	              ) b
	          ) b 
	          left join (
	            select 
	              vehicle_number, 
	              completed_ts
	            from 
	              (
	                select 
	                  trim(b.vehicle_number) as vehicle_number, 
	                  trunc(b.completed_ts) as completed_ts, 
	                  '(' || b.site_id || ' - ' || to_char(
	                    b.completed_ts, 'dd-Mon-yyyy hh24:mi:ss'
	                  ) || ' - ' || b.delivery_volume || ' L)' as rmk 
	                from 
	                  (
	                    select 
	                      b.*, 
	                      dense_rank() over(
	                        partition by trim(b.vehicle_number) 
	                        order by 
	                          delivery_volume desc, 
	                          b.completed_ts
	                      ) as drank 
	                    from 
	                      iem_vw_iedcc_det_catatan_transaksi b 
	                    where 
	                      trunc(b.completed_ts) = trunc(cur_date.current_date) ---1 
	                      and b.vehicle_number is not null 
	                    order by 
	                      delivery_volume desc
	                  ) b 
	                where 
	                  drank <= 3
	              ) b 
	            group by 
	              vehicle_number, 
	              completed_ts
	          ) c on b.vehicle_number = c.vehicle_number 
	          and b.date_ts = c.completed_ts 
	        where 
	          drank <= 1
	      ) b 
	    where 
	      jumlah >= 1 
	      and b.delivery_volume2 > 200 
	      and b.delivery_value2 > 0 
	      and b.vehicle_number is not null
	  ) b 
	  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id 
	GROUP BY 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM;
	 COMMIT;
	
	--PICCST024
	INSERT INTO APP_IEM_INOVASI.NEW_VALUE_CREATION
	select 
	  'INOVASI' USE_CASE, 
	  'SPBU' SUB_USE_CASE, 
	  'PICCST024' EXCEPT_CODE, 
	  'Transaksi Solar Nopol dan NIK Kosong Post Purchase' EXCEPT_NAME, 
	  'NOMOR SPBU' "OBJECT", 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM, 
	  TRUNC(
	    max(b.completed_ts)
	  ) as date_ts, 
	  max(grade_name_std) as grade_name_std, 
	  sum(b.delivery_volume) as delivery_volume, 
	  sum(b.delivery_value) as delivery_value, 
	  count(DISTINCT b.site_id) as jumlah 
	from 
	  (
	    select 
	      b.*, 
	      c.tipe_agen, 
	      c.alamat, 
	      c.propinsi, 
	      c.kota, 
	      c.regional, 
	      dense_rank() over(
	        partition by c.regional 
	        order by 
	          b.delivery_volume desc
	      ) as drank 
	    from 
	      (
	        select 
	          b.site_id, 
	          b.vehicle_type, 
	          max(b.completed_ts) as completed_ts, 
	          max(grade_name_std) as grade_name_std, 
	          sum(b.delivery_volume) as delivery_volume, 
	          sum(b.delivery_value) as delivery_value, 
	          count(*) as jumlah 
	        from 
	          iem_vw_iedcc_det_catatan_transaksi b 
	        where 
	          trunc(b.completed_ts) = trunc(cur_date.current_date) ---1 
	          and b.grade_name_std = 'BIO_SOLAR' 
	          and b.delivery_volume > 0 
	          and (
	            b.vehicle_type = 5 
	            or b.vehicle_type is null
	          ) 
	          and b.delivery_value > 0 
	          and (
	            length(b.agency_name) < 10 
	            or b.agency_name is null
	          ) 
	          and trim(b.VEHICLE_NUMBER) is null 
	        group by 
	          b.site_id, 
	          b.vehicle_type
	      ) b 
	      left join IEM_TBL_IEDCC_MASTER_SPBU c on b.site_id = c.site_id
	  ) b 
	  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id 
	GROUP BY 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM;
	 COMMIT;
	
	--PICCST025
	INSERT INTO APP_IEM_INOVASI.NEW_VALUE_CREATION
	SELECT 
	  'INOVASI' USE_CASE, 
	  'SPBU' SUB_USE_CASE, 
	  'PICCST025' EXCEPT_CODE, 
	  'Transaksi Solar BBM > 50 Liter dan No Pol Tidak Lengkap' EXCEPT_NAME, 
	  'NOMOR SPBU' "OBJECT", 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM, 
	  TRUNC(
	    max(b.completed_ts)
	  ) as date_ts, 
	  max(grade_name_std) as grade_name_std, 
	  sum(b.delivery_volume) as delivery_volume, 
	  sum(b.delivery_value) as delivery_value, 
	  count(DISTINCT b.site_id) as jumlah 
	FROM 
	  (
	    select 
	      b.*, 
	      c.tipe_agen, 
	      c.alamat, 
	      c.propinsi, 
	      c.kota, 
	      c.regional, 
	      dense_rank() over(
	        partition by c.regional 
	        order by 
	          b.delivery_volume desc
	      ) as drank 
	    from 
	      (
	        select 
	          b.site_id, 
	          max(b.completed_ts) as completed_ts, 
	          max(grade_name_std) as grade_name_std, 
	          sum(b.delivery_volume) as delivery_volume, 
	          sum(b.delivery_value) as delivery_value, 
	          count(*) as jumlah 
	        from 
	          iem_vw_iedcc_det_catatan_transaksi b 
	          left join (
	            select 
	              distinct nopol 
	            from 
	              iem_tbl_spbu_nopol_pengecualian 
	            where 
	              trunc(ticket_Date) >= trunc(cur_date.current_date)---30
	          ) n on trim(b.vehicle_number)= trim(n.nopol) 
	          left join (
	            select 
	              distinct nopol 
	            from 
	              iem_tbl_spbu_signal_action 
	            where 
	              response in (
	                'Recomendasi', 'Transaksi Wajar'
	              ) 
	              and trunc(cur_date.current_date) between trunc(start_date) 
	              and trunc(end_date)
	          ) rr on trim(b.vehicle_number)= trim(rr.nopol) 
	        where 
	          trunc(b.completed_ts) = trunc(cur_date.current_date)---1 
	          and b.grade_name_std = 'BIO_SOLAR' 
	          and b.delivery_volume > 50 
	          and b.delivery_value > 0 
	          and n.nopol is null 
	          and rr.nopol is null 
	          and (
	            length(b.agency_name) < 10 
	            or b.agency_name is null
	          ) 
	          and (
	            (
	              length(
	                trim(b.VEHICLE_NUMBER)
	              ) between 1 
	              and 4
	            ) 
	            or REGEXP_LIKE(
	              trim(b.VEHICLE_NUMBER), 
	              '^[0-9]+$'
	            ) 
	            or trim(b.vehicle_number) in (
	              'R3KOM', 'P374NI', 'NE74YAN', 'F0000PTN', 
	              'NOMOR50022', 'P526MMB501', 'NOMOR50021', 
	              'R3COM', 'DP65J'
	            )
	          ) 
	        group by 
	          b.site_id
	      ) b 
	      left join IEM_TBL_IEDCC_MASTER_SPBU c on b.site_id = c.site_id
	  ) b 
	  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id 
	where 
	  drank <= 10 
	GROUP BY  
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM;
	 COMMIT;
	
	--PICCST028(done)
	INSERT INTO APP_IEM_INOVASI.NEW_VALUE_CREATION
	SELECT 
	  'INOVASI' USE_CASE, 
	  'SPBU' SUB_USE_CASE, 
	  'PICCST028' EXCEPT_CODE, 
	  'Transaksi Pertalite tidak wajar > 100 liter' EXCEPT_NAME, 
	  'NOMOR SPBU' "OBJECT", 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM, 
	  TRUNC(
	    max(b.completed_ts)
	  ) as date_ts, 
	  max(grade_name_std) as grade_name_std, 
	  sum(b.delivery_volume) as delivery_volume, 
	  sum(b.delivery_value) as delivery_value, 
	  count(DISTINCT b.site_id) as jumlah 
	FROM 
	  (
	    select 
	      b.*, 
	      c.tipe_agen, 
	      c.alamat, 
	      c.propinsi, 
	      c.kota, 
	      c.regional, 
	      dense_rank() over(
	        partition by c.regional 
	        order by 
	          b.delivery_volume desc
	      ) as drank 
	    from 
	      (
	        select 
	          b.site_id, 
	          max(b.completed_ts) as completed_ts, 
	          max(grade_name_std) as grade_name_std, 
	          sum(b.delivery_volume) as delivery_volume, 
	          sum(b.delivery_value) as delivery_value, 
	          count(*) as jumlah 
	        from 
	          iem_vw_iedcc_det_catatan_transaksi_pertalite b 
	          left join (
	            select 
	              distinct nopol 
	            from 
	              iem_tbl_spbu_nopol_pengecualian 
	            where 
	              trunc(ticket_Date) >= trunc(cur_date.current_date)---30
	          ) n on trim(b.vehicle_number)= trim(n.nopol) 
	          left join (
	            select 
	              distinct nopol 
	            from 
	              iem_tbl_spbu_signal_action 
	            where 
	              response in (
	                'Recomendasi', 'Transaksi Wajar'
	              ) 
	              and trunc(cur_date.current_date) between trunc(start_date) 
	              and trunc(end_date)
	          ) rr on trim(b.vehicle_number)= trim(rr.nopol) 
	        where 
	          trunc(b.completed_ts) = trunc(cur_date.current_date)---1 
	          and b.delivery_volume >= 101 
	          and b.delivery_value > 0 
	          and n.nopol is null 
	          and rr.nopol is null 
	          and (
	            length(b.agency_name) < 10 
	            or b.agency_name is null
	          ) 
	        group by 
	          b.site_id
	      ) b 
	      left join IEM_TBL_IEDCC_MASTER_SPBU c on b.site_id = c.site_id 
	    where 
	      REGEXP_LIKE(b.site_id, '^[0-9]+$')
	  ) b 
	  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id 
	GROUP BY 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM;
	 COMMIT;
	
	--PICCST067(done)
	INSERT INTO APP_IEM_INOVASI.NEW_VALUE_CREATION
	SELECT 
	  'INOVASI' USE_CASE, 
	  'SPBU' SUB_USE_CASE, 
	  'PICCST067' EXCEPT_CODE, 
	  'Transaksi Repetitive Biosolar Tidak Wajar' EXCEPT_NAME, 
	  'NOMOR SPBU' "OBJECT", 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM, 
	  TRUNC(
	    max(b.date_ts)
	  ) as date_ts, 
	  max(grade_name_std) as grade_name_std, 
	  sum(b.delivery_volume) as delivery_volume, 
	  sum(b.delivery_value) as delivery_value, 
	  count(DISTINCT b.site_id) as jumlah 
	FROM 
	  (
	    select 
	      b.*, 
	      c.regional, 
	      c.tipe_agen, 
	      c.kota, 
	      c.propinsi, 
	      c.alamat, 
	      dense_rank() over(
	        partition by c.regional 
	        order by 
	          b.delivery_volume desc
	      ) as drank2 
	    from 
	      (
	        select 
	          trunc(b.date_ts) as date_ts, 
	          b.site_id, 
	          b.grade_name_std, 
	          sum(delivery_volume) as delivery_volume, 
	          sum(delivery_value) as delivery_value, 
	          sum(jlh) as jlh
	        from 
	          (
	            select 
	              trunc(b.completed_ts) as date_ts, 
	              b.site_id, 
	              b.grade_name_std, 
	              sum(delivery_volume) as delivery_volume, 
	              sum(delivery_value) as delivery_value, 
	              count(*) as jlh
	            from 
	              (
	                select 
	                  b.*, 
	                  '(' || nvl(
	                    trim(b.VEHICLE_NUMBER), 
	                    b.AGENCY_NAME
	                  ) || ' - ' || to_char(b.completed_ts, 'hh24:mi:ss') || ' - ' || b.delivery_volume || ' L)' as rmk 
	                from 
	                  (
	                    select 
	                      b.*, 
	                      sum(b.DELIVERY_VOLUME) over(
	                        partition by trunc(b.COMPLETED_TS), 
	                        b.site_id, 
	                        b.HOSE_NUMBER, 
	                        b.PUMP_NAME, 
	                        b.GRADE_NAME_STD, 
	                        b.DELIVERY_VOLUME, 
	                        jenis, 
	                        id_volume2
	                      ) as sum_volume, 
	                      sum(b.DELIVERY_VALUE) over(
	                        partition by trunc(b.COMPLETED_TS), 
	                        b.site_id, 
	                        b.HOSE_NUMBER, 
	                        b.PUMP_NAME, 
	                        b.GRADE_NAME_STD, 
	                        b.DELIVERY_VOLUME, 
	                        jenis, 
	                        id_volume2
	                      ) as sum_value, 
	                      count(*) over(
	                        partition by trunc(b.COMPLETED_TS), 
	                        b.site_id, 
	                        b.HOSE_NUMBER, 
	                        b.PUMP_NAME, 
	                        b.GRADE_NAME_STD, 
	                        b.DELIVERY_VOLUME, 
	                        jenis, 
	                        id_volume2
	                      ) as jlh 
	                    from 
	                      (
	                        select 
	                          b.*, 
	                          LAST_VALUE(b.menit2 IGNORE NULLS) OVER(
	                            partition by trunc(b.COMPLETED_TS), 
	                            b.site_id, 
	                            b.HOSE_NUMBER, 
	                            b.PUMP_NAME, 
	                            b.GRADE_NAME_STD, 
	                            b.DELIVERY_VOLUME, 
	                            jenis 
	                            ORDER BY 
	                              b.COMPLETED_TS desc
	                          ) ID_VOLUME2, 
	                          LAST_VALUE(b.menit3 IGNORE NULLS) OVER(
	                            partition by trunc(b.COMPLETED_TS), 
	                            b.site_id, 
	                            b.HOSE_NUMBER, 
	                            b.PUMP_NAME, 
	                            b.GRADE_NAME_STD, 
	                            b.DELIVERY_VOLUME, 
	                            jenis 
	                            ORDER BY 
	                              b.COMPLETED_TS desc
	                          ) ID_VOLUME3, 
	                          LAST_VALUE(b.menit4 IGNORE NULLS) OVER(
	                            partition by trunc(b.COMPLETED_TS), 
	                            b.site_id, 
	                            b.HOSE_NUMBER, 
	                            b.PUMP_NAME, 
	                            b.GRADE_NAME_STD, 
	                            b.DELIVERY_VOLUME, 
	                            jenis 
	                            ORDER BY 
	                              b.COMPLETED_TS desc
	                          ) ID_VOLUME4, 
	                          LAST_VALUE(b.menit5 IGNORE NULLS) OVER(
	                            partition by trunc(b.COMPLETED_TS), 
	                            b.site_id, 
	                            b.HOSE_NUMBER, 
	                            b.PUMP_NAME, 
	                            b.GRADE_NAME_STD, 
	                            b.DELIVERY_VOLUME, 
	                            jenis 
	                            ORDER BY 
	                              b.COMPLETED_TS desc
	                          ) ID_VOLUME5 
	                        from 
	                          (
	                            select 
	                              b.*, 
	                              (COMPLETED_TS - bf_r1) * 24 * 60 as menit1, 
	                              b.COMPLETED_TS as trx, 
	                              (COMPLETED_TS - bf_r2) * 24 * 60 as menit2, 
	                              (COMPLETED_TS - bf_r3) * 24 * 60 as menit3, 
	                              (COMPLETED_TS - bf_r4) * 24 * 60 as menit4, 
	                              (COMPLETED_TS - bf_r5) * 24 * 60 as menit5 
	                            from 
	                              (
	                                select 
	                                  b.*, 
	                                  lag(b.COMPLETED_TS, 1) over(
	                                    partition by trunc(b.COMPLETED_TS), 
	                                    b.site_id, 
	                                    b.HOSE_NUMBER, 
	                                    b.PUMP_NAME, 
	                                    b.GRADE_NAME_STD, 
	                                    b.DELIVERY_VOLUME, 
	                                    jenis 
	                                    order by 
	                                      b.COMPLETED_TS
	                                  ) as bf_r1, 
	                                  lag(b.COMPLETED_TS, 2) over(
	                                    partition by trunc(b.COMPLETED_TS), 
	                                    b.site_id, 
	                                    b.HOSE_NUMBER, 
	                                    b.PUMP_NAME, 
	                                    b.GRADE_NAME_STD, 
	                                    b.DELIVERY_VOLUME, 
	                                    jenis 
	                                    order by 
	                                      b.COMPLETED_TS
	                                  ) as bf_r2, 
	                                  lag(b.COMPLETED_TS, 3) over(
	                                    partition by trunc(b.COMPLETED_TS), 
	                                    b.site_id, 
	                                    b.HOSE_NUMBER, 
	                                    b.PUMP_NAME, 
	                                    b.GRADE_NAME_STD, 
	                                    b.DELIVERY_VOLUME, 
	                                    jenis 
	                                    order by 
	                                      b.COMPLETED_TS
	                                  ) as bf_r3, 
	                                  lag(b.COMPLETED_TS, 4) over(
	                                    partition by trunc(b.COMPLETED_TS), 
	                                    b.site_id, 
	                                    b.HOSE_NUMBER, 
	                                    b.PUMP_NAME, 
	                                    b.GRADE_NAME_STD, 
	                                    b.DELIVERY_VOLUME, 
	                                    jenis 
	                                    order by 
	                                      b.COMPLETED_TS
	                                  ) as bf_r4, 
	                                  lag(b.COMPLETED_TS, 5) over(
	                                    partition by trunc(b.COMPLETED_TS), 
	                                    b.site_id, 
	                                    b.HOSE_NUMBER, 
	                                    b.PUMP_NAME, 
	                                    b.GRADE_NAME_STD, 
	                                    b.DELIVERY_VOLUME, 
	                                    jenis 
	                                    order by 
	                                      b.COMPLETED_TS
	                                  ) as bf_r5 
	                                from 
	                                  (
	                                    select 
	                                      b.*, 
	                                      case when length(b.agency_name) > 10 then 2 else 1 end as jenis 
	                                    from 
	                                      iem_vw_iedcc_det_catatan_transaksi b 
	                                    where 
	                                      trunc(b.completed_ts)= trunc(cur_date.current_date)---1 
	                                      and b.DELIVERY_VOLUME > 10 
	                                      AND GRADE_NAME_STD = 'BIO_SOLAR' --and length(b.agency_name) > 10
	                                      ) b
	                              ) b
	                          ) b
	                      ) b 
	                    where 
	                      (
	                        id_volume2 <= 8 
	                        or id_volume3 <= 8 
	                        or id_volume4 <= 8 
	                        or id_volume5 <= 8
	                      )
	                  ) b 
	                where 
	                  sum_volume >= 61 
	                  and jlh = 3
	              ) b 
	            group by 
	              trunc(b.completed_ts), 
	              b.site_id, 
	              b.grade_name_std
	          ) b 
	        group by 
	          date_ts, 
	          b.site_id, 
	          b.grade_name_std
	      ) b 
	      left join IEM_TBL_IEDCC_MASTER_SPBU c on b.site_id = c.site_id 
	    where 
	      REGEXP_LIKE(b.site_id, '^[0-9]+$') 
	      and jlh = 3 
	      and c.flag = 'Tahap 1'
	  ) b 
	  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id 
	GROUP BY 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM;
	 COMMIT;
	
	--PICCST068(done)
	INSERT INTO APP_IEM_INOVASI.NEW_VALUE_CREATION
	SELECT 
	  'INOVASI' USE_CASE, 
	  'SPBU' SUB_USE_CASE, 
	  'PICCST068' EXCEPT_CODE, 
	  'Tera Tidak Wajar' EXCEPT_NAME, 
	  'NOMOR TERA' "OBJECT", 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM, 
	  TRUNC(
	    max(b.completed_ts)
	  ) as date_ts, 
	  max(grade_name_std) as grade_name_std, 
	  sum(b.delivery_volume) as delivery_volume, 
	  sum(b.delivery_value) as delivery_value, 
	  count(
	    DISTINCT trim(b.vehicle_number)
	  ) as jumlah 
	FROM 
	  (
	    select 
	      b.*, 
	      c.regional, 
	      c.tipe_agen, 
	      c.kota, 
	      c.propinsi, 
	      c.alamat, 
	      dense_rank() over(
	        partition by c.regional 
	        order by 
	          b.delivery_volume desc
	      ) as drank2 
	    from 
	      (
	        select 
	          trunc(b.completed_ts) as date_ts, 
	          min(b.completed_ts) as min_completed_ts, 
	          max(b.completed_ts) as completed_ts, 
	          trim(b.vehicle_number) as vehicle_number, 
	          max(b.site_id) as site_id, 
	          b.grade_name_std, 
	          sum(b.delivery_volume) as delivery_volume, 
	          sum(b.delivery_value) as delivery_value, 
	          count(*) as jumlah 
	        from 
	          iem_vw_iedcc_det_catatan_transaksi_subsidi b 
	        where 
	          trunc(b.completed_ts) = trunc(cur_date.current_date) ---1 
	          and b.delivery_volume > 25.1 
	          and (
	            trim(b.vehicle_number) like 'TP%' 
	            or trim(b.vehicle_number) like 'TA%' 
	            or trim(b.vehicle_number) like 'TW%' 
	            or trim(b.vehicle_number) like 'TB%' 
	            or trim(b.vehicle_number) like 'TE%' 
	            or trim(b.vehicle_number) like 'TM%' 
	            or trim(b.vehicle_number) like 'TQ%' 
	            or trim(b.vehicle_number) like 'TR%' 
	            or trim(b.vehicle_number) like 'TX%' 
	            or trim(b.vehicle_number) like 'TY%'
	          ) 
	        group by 
	          trunc(b.completed_ts), 
	          trim(b.vehicle_number), 
	          b.grade_name_std
	      ) b 
	      left join IEM_TBL_IEDCC_MASTER_SPBU c on b.site_id = c.site_id 
	    where 
	      REGEXP_LIKE(b.site_id, '^[0-9]+$')
	  ) b 
	  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id 
	GROUP BY 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM;
	 COMMIT;
	  
	--PICCST064(done)
	INSERT INTO APP_IEM_INOVASI.NEW_VALUE_CREATION
	SELECT 
	  'INOVASI' USE_CASE, 
	  'SPBU' SUB_USE_CASE, 
	  'PICCST064' EXCEPT_CODE, 
	  'Transaksi rekomendasi di daerah tidak wajar' EXCEPT_NAME, 
	  'NOMOR IDENTITAS' "OBJECT", 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM, 
	  TRUNC(
	    max(b.completed_ts)
	  ) as date_ts, 
	  max(grade_name_std) as grade_name_std, 
	  sum(b.delivery_volume) as delivery_volume, 
	  sum(b.delivery_value) as delivery_value, 
	  count(
	    DISTINCT trim(b.agency_name)
	  ) as jumlah 
	FROM 
	  (
	    select 
	      b.* 
	    from 
	      (
	        select 
	          agency_name, 
	          GRADE_NAME_STD, 
	          max(site_id) as site_id, 
	          COMPLETED_TS, 
	          sum(b.DELIVERY_VOLUME) as DELIVERY_VOLUME, 
	          sum(b.DELIVERY_VALUE) as DELIVERY_VALUE, 
	          sum(jumlah) as jumlah, 
	          LISTAGG(distinct site_id, '##') WITHIN GROUP(
	            ORDER BY 
	              site_id
	          ) as site_ids, 
	          LISTAGG(distinct kota, '##') WITHIN GROUP(
	            ORDER BY 
	              kota
	          ) as kotas, 
	          listagg(rmk, ' ') within group (
	            order by 
	              completed_ts
	          ) as rmk 
	        from 
	          (
	            select 
	              b.*, 
	              '(' || b.SITE_ID || ' - ' || b.kota || ' - ' || b.delivery_volume || ' L)' as rmk 
	            from 
	              (
	                select 
	                  agency_name, 
	                  GRADE_NAME_STD, 
	                  trunc(b.COMPLETED_TS) as COMPLETED_TS, 
	                  max(b.COMPLETED_TS) as max_COMPLETED_TS, 
	                  sum(b.DELIVERY_VOLUME) as DELIVERY_VOLUME, 
	                  sum(b.DELIVERY_VALUE) as DELIVERY_VALUE, 
	                  b.site_id, 
	                  d.kota, 
	                  count(*) as jumlah 
	                from 
	                  (
	                    select 
	                      * 
	                    from 
	                      iem_vw_iedcc_det_catatan_transaksi b 
	                    where 
	                      trunc(b.COMPLETED_TS) = trunc(cur_date.current_date)---1 
	                      and length(b.agency_name) > 10
	                  ) b 
	                  left join (
	                    select 
	                      site_id, 
	                      max(kota) as kota 
	                    from 
	                      IEM_TBL_IEDCC_MASTER_SPBU d 
	                    group by 
	                      site_id
	                  ) d on b.SITE_ID = d.site_id 
	                group by 
	                  agency_name, 
	                  GRADE_NAME_STD, 
	                  trunc(b.COMPLETED_TS), 
	                  b.site_id, 
	                  d.kota
	              ) b
	          ) b 
	        group by 
	          agency_name, 
	          GRADE_NAME_STD, 
	          COMPLETED_TS
	      ) b 
	    where 
	      kotas like '%##%'
	  ) b 
	  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id 
	where 
	  REGEXP_LIKE(b.site_id, '^[0-9]+$') 
	GROUP BY 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM;
	 COMMIT;
	
	--PICCST070(done)
	INSERT INTO APP_IEM_INOVASI.NEW_VALUE_CREATION
	select 
	  'INOVASI' USE_CASE, 
	  'SPBU' SUB_USE_CASE, 
	  'PICCST070' EXCEPT_CODE, 
	  'Transaksi rekomendasi di daerah tidak wajar' EXCEPT_NAME, 
	  'NOMOR SPBU' "OBJECT", 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM, 
	  TRUNC(
	    max(b.date_ts)
	  ) as date_ts, 
	  max(grade_name_std) as grade_name_std, 
	  sum(b.delivery_volume) as delivery_volume, 
	  sum(b.delivery_value) as delivery_value, 
	  count(
	    DISTINCT trim(b.site_id)
	  ) as jumlah 
	from 
	  (
	    select 
	      * 
	    from 
	      (
	        select 
	          trunc(b.completed_ts) as date_ts, 
	          b.site_id, 
	          b.grade_name_std, 
	          jam_buka_ops, 
	          jam_tutup, 
	          sum(b.delivery_volume) as delivery_volume, 
	          sum(b.delivery_value) as delivery_value, 
	          count(*) as jlh 
	        from 
	          (
	            select 
	              b.*, 
	              case when jam between jam_buka_ops 
	              and jam_tutup_ops then 1 else 0 end as wajar 
	            from 
	              (
	                select 
	                  b.*, 
	                  to_char(b.COMPLETED_TS, 'hh24:mi:ss') as jam, 
	                  c.jam_buka_ops, 
	                  c.jam_tutup_ops as jam_tutup, 
	                  case when c.jam_tutup_ops <> '00:00:00' then to_char(
	                    to_date(
	                      '20-Sep-2023 ' || c.jam_tutup_ops, 
	                      'dd-Mon-yyyy hh24:mi:ss'
	                    ) + (.000694 * 16), 
	                    'hh24:mi:ss'
	                  ) else c.jam_tutup_ops end as jam_tutup_ops 
	                from 
	                  iem_vw_iedcc_det_catatan_transaksi b 
	                  left join (
	                    select 
	                      SITE_ID, 
	                      NAME_SPBU, 
	                      ADDRESS, 
	                      LATITUDE, 
	                      LONGITUDE, 
	                      MOR, 
	                      CODE_MOR, 
	                      TREG, 
	                      PROVINCE, 
	                      CODE_PROVINCE, 
	                      SAM, 
	                      CODE_SAM, 
	                      SBM, 
	                      CODE_SBM, 
	                      CITY, 
	                      CODE_CITY, 
	                      TOLL, 
	                      WISATA, 
	                      UAT, 
	                      TYPE_SPBU, 
	                      JAMALI, 
	                      JALUR_MUDIK, 
	                      INACTIVE, 
	                      JAM_BUKA_OPS, 
	                      case when JAM_TUTUP_OPS = '24:00:00' then '00:00:00' else JAM_TUTUP_OPS end JAM_TUTUP_OPS, 
	                      NETWORK_TYPE, 
	                      SYSTEM_USE, 
	                      INSERT_DATE, 
	                      LAST_HEARTBEAT, 
	                      FLAG 
	                    from 
	                      IEM_TBL_IEDCC_SPBU_PROFILE c 
	                    where 
	                      jam_tutup_ops <> jam_buka_ops
	                  ) c on b.SITE_ID = c.site_id 
	                where 
	                  trunc(b.COMPLETED_TS)= trunc(cur_date.current_date)---1 
	                  and jam_buka_ops <> '00:00:00'
	              ) b 
	            where 
	              jam_tutup_ops <> '00:00:00'
	          ) b 
	        where 
	          wajar = 0 
	        group by 
	          trunc(b.completed_ts), 
	          b.site_id, 
	          b.grade_name_std, 
	          jam_buka_ops, 
	          jam_tutup
	      ) b 
	    where 
	      b.delivery_volume >= 201
	  ) b 
	  left join (
	    select 
	      b.SITE_ID, 
	      trunc(b.COMPLETED_TS) as completed_ts, 
	      listagg(rmk, ' ') within group (
	        order by 
	          drank
	      ) as remak 
	    from 
	      (
	        select 
	          b.*, 
	          dense_rank() over(
	            partition by b.SITE_ID 
	            order by 
	              b.DELIVERY_VOLUME desc, 
	              b.COMPLETED_TS desc
	          ) as drank, 
	          '(' || b.jam || ' = ' || b.DELIVERY_VOLUME || ' L; NOPOL = ' || trim(b.VEHICLE_NUMBER) || ') ' as rmk 
	        from 
	          (
	            select 
	              b.*, 
	              case when jam between jam_buka_ops 
	              and jam_tutup_ops then 1 else 0 end as wajar 
	            from 
	              (
	                select 
	                  b.*, 
	                  to_char(b.COMPLETED_TS, 'hh24:mi:ss') as jam, 
	                  c.jam_buka_ops, 
	                  case when c.jam_tutup_ops <> '00:00:00' then to_char(
	                    to_date(
	                      '20-Sep-2023 ' || c.jam_tutup_ops, 
	                      'dd-Mon-yyyy hh24:mi:ss'
	                    ) + (.000694 * 16), 
	                    'hh24:mi:ss'
	                  ) else c.jam_tutup_ops end as jam_tutup_ops 
	                from 
	                  iem_vw_iedcc_det_catatan_transaksi b 
	                  left join (
	                    select 
	                      SITE_ID, 
	                      NAME_SPBU, 
	                      ADDRESS, 
	                      LATITUDE, 
	                      LONGITUDE, 
	                      MOR, 
	                      CODE_MOR, 
	                      TREG, 
	                      PROVINCE, 
	                      CODE_PROVINCE, 
	                      SAM, 
	                      CODE_SAM, 
	                      SBM, 
	                      CODE_SBM, 
	                      CITY, 
	                      CODE_CITY, 
	                      TOLL, 
	                      WISATA, 
	                      UAT, 
	                      TYPE_SPBU, 
	                      JAMALI, 
	                      JALUR_MUDIK, 
	                      INACTIVE, 
	                      JAM_BUKA_OPS, 
	                      case when JAM_TUTUP_OPS = '24:00:00' then '00:00:00' else JAM_TUTUP_OPS end JAM_TUTUP_OPS, 
	                      NETWORK_TYPE, 
	                      SYSTEM_USE, 
	                      INSERT_DATE, 
	                      LAST_HEARTBEAT, 
	                      FLAG 
	                    from 
	                      IEM_TBL_IEDCC_SPBU_PROFILE c 
	                    where 
	                      jam_tutup_ops <> jam_buka_ops
	                  ) c on b.SITE_ID = c.site_id 
	                where 
	                  trunc(b.COMPLETED_TS)= trunc(cur_date.current_date)---1 
	                  and jam_buka_ops <> '00:00:00'
	              ) b 
	            where 
	              jam_tutup_ops <> '00:00:00'
	          ) b 
	        where 
	          wajar = 0
	      ) b 
	    where 
	      drank <= 3 
	    group by 
	      b.SITE_ID, 
	      trunc(b.COMPLETED_TS)
	  ) e on b.SITE_ID = e.site_id 
	  and b.date_ts = e.COMPLETED_TS 
	  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z on b.site_id = z.site_id 
	where 
	  REGEXP_LIKE(b.site_id, '^[0-9]+$') 
	GROUP BY 
	  z.REGIONAL, 
	  z.SBM, 
	  z.SAM;
	 COMMIT;
	
  END LOOP;
END;
  