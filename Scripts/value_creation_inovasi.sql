--PICCST022(done)
SELECT
  TRUNC(
    max(b.completed_ts)
  ) as date_ts,
  JSON_OBJECT(
  'SUB_HOLDING' VALUE 'COMMERCIAL & TRADING',
  'USE_CASE' VALUE 'INOVASI',
  'SUB_USE_CASE' VALUE 'SPBU',
  'EXCEPT_CODE' VALUE 'PICCST022',
  'EXCEPT_NAME' VALUE 'Transaksi Solar tidak wajar > 200 liter',
  'REGIONAL' VALUE z.REGIONAL,
  'SBM' VALUE z.SBM,
  'SAM' VALUE z.SAM,
  'OBJECT_DETAIL' VALUE JSON_OBJECT(
  'SPBU' VALUE b.site_id,
  'PROVINSI' VALUE z.propinsi,
  'GRADE_NAME_STD' VALUE max(b.grade_name_std),
  'TRANSACTION_VOLUME' VALUE sum(b.delivery_volume),
  'TRANSACTION_VALUE' VALUE sum(b.delivery_value),
  'COUNT_VEHICLE' VALUE count(DISTINCT b.VEHICLE_NUMBER)
  )
  ) DATA_DETAIL
from
  iem_vw_iedcc_det_catatan_transaksi b
  left join (
    select distinct
      nopol
    from
      iem_tbl_spbu_nopol_pengecualian
    where
      trunc (ticket_Date) >= trunc (sysdate) -30
  ) n on trim(b.vehicle_number) = trim(n.nopol)
  left join (
    select distinct
      nopol
    from
      iem_tbl_spbu_signal_action
    where
      response in ('Recomendasi', 'Transaksi Wajar')
      and trunc (sysdate) between trunc (start_date) and trunc  (end_date)
  ) rr on trim(b.vehicle_number) = trim(rr.nopol)
  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id
where
  trunc (b.completed_ts) = trunc (sysdate) -1
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
  z.SAM, 
  b.site_id,
  z.propinsi;
 COMMIT;
  
--PICCST023(done)
select  
  TRUNC(
    max(b.completed_ts)
  ) as date_ts,
  JSON_OBJECT(
  'SUB_HOLDING' VALUE 'COMMERCIAL & TRADING',
  'USE_CASE' VALUE 'INOVASI',
  'SUB_USE_CASE' VALUE 'SPBU',
  'EXCEPT_CODE' VALUE 'PICCST023',
  'EXCEPT_NAME' VALUE 'Transaksi Solar berulang pada no polisi sama di hari yang sama > 200 liter',
  'REGIONAL' VALUE z.REGIONAL,
  'SBM' VALUE z.SBM,
  'SAM' VALUE z.SAM,
  'OBJECT_DETAIL' VALUE JSON_OBJECT(
  'SPBU' VALUE b.site_id,
  'PROVINSI' VALUE z.propinsi,
  'GRADE_NAME_STD' VALUE max(b.grade_name_std),
  'TRANSACTION_VOLUME' VALUE sum(b.delivery_volume),
  'TRANSACTION_VALUE' VALUE sum(b.delivery_value),
  'COUNT_VEHICLE' VALUE count(DISTINCT b.VEHICLE_NUMBER)
  )
  ) DATA_DETAIL 
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
                      trunc(ticket_Date) >= trunc(sysdate)-30
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
                      and trunc(sysdate) between trunc(start_date) 
                      and trunc(end_date)
                  ) rr on trim(b.vehicle_number)= trim(rr.nopol) 
                where 
                  trunc(b.completed_ts) = trunc(sysdate) -1 
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
                  trunc(b.completed_ts) as completed_ts
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
                      trunc(b.completed_ts) = trunc(sysdate) -1 
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
  z.SAM, 
  b.site_id,
  z.propinsi;
 COMMIT;

--PICCST024(done)
select  
  TRUNC(b.completed_ts) as date_ts,
  JSON_OBJECT(
  'SUB_HOLDING' VALUE 'COMMERCIAL & TRADING',
  'USE_CASE' VALUE 'INOVASI',
  'SUB_USE_CASE' VALUE 'SPBU',
  'EXCEPT_CODE' VALUE 'PICCST024',
  'EXCEPT_NAME' VALUE 'Transaksi Solar Nopol dan NIK Kosong Post Purchase',
  'REGIONAL' VALUE z.REGIONAL,
  'SBM' VALUE z.SBM,
  'SAM' VALUE z.SAM,
  'OBJECT_DETAIL' VALUE JSON_OBJECT(
  'SPBU' VALUE b.site_id,
  'PROVINSI' VALUE z.propinsi,
  'GRADE_NAME_STD' VALUE b.grade_name_std,
  'TRANSACTION_VOLUME' VALUE b.delivery_volume,
  'TRANSACTION_VALUE' VALUE b.delivery_value,
  'COUNT_VEHICLE' VALUE b.jumlah
  )
  ) DATA_DETAIL 
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
          trunc(b.completed_ts) = trunc(sysdate) -1 
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
  z.SAM, 
  b.site_id,
  z.propinsi,
  b.grade_name_std,
  b.completed_ts,
  b.delivery_volume,
  b.delivery_value,
  b.jumlah;
 COMMIT;

--PICCST025(done)
SELECT 
  TRUNC(b.completed_ts) as date_ts,
  JSON_OBJECT(
  'SUB_HOLDING' VALUE 'COMMERCIAL & TRADING',
  'USE_CASE' VALUE 'INOVASI',
  'SUB_USE_CASE' VALUE 'SPBU',
  'EXCEPT_CODE' VALUE 'PICCST025',
  'EXCEPT_NAME' VALUE 'Transaksi Solar BBM > 50 Liter dan No Pol Tidak Lengkap',
  'REGIONAL' VALUE z.REGIONAL,
  'SBM' VALUE z.SBM,
  'SAM' VALUE z.SAM,
  'OBJECT_DETAIL' VALUE JSON_OBJECT(
  'SPBU' VALUE b.site_id,
  'PROVINSI' VALUE z.propinsi,
  'GRADE_NAME_STD' VALUE b.grade_name_std,
  'TRANSACTION_VOLUME' VALUE b.delivery_volume,
  'TRANSACTION_VALUE' VALUE b.delivery_value,
  'COUNT_VEHICLE' VALUE b.jumlah
  )
  ) DATA_DETAIL 
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
              trunc(ticket_Date) >= trunc(sysdate)-30
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
              and trunc(sysdate) between trunc(start_date) 
              and trunc(end_date)
          ) rr on trim(b.vehicle_number)= trim(rr.nopol) 
        where 
          trunc(b.completed_ts) = trunc(sysdate)-1 
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
  drank <= 10;
 COMMIT;

--PICCST028
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
              trunc(ticket_Date) >= trunc(sysdate)-30
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
              and trunc(sysdate) between trunc(start_date) 
              and trunc(end_date)
          ) rr on trim(b.vehicle_number)= trim(rr.nopol) 
        where 
          trunc(b.completed_ts) = trunc(sysdate)-1 
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
SELECT  
  b.date_ts,
  JSON_OBJECT(
  'SUB_HOLDING' VALUE 'COMMERCIAL & TRADING',
  'USE_CASE' VALUE 'INOVASI',
  'SUB_USE_CASE' VALUE 'SPBU',
  'EXCEPT_CODE' VALUE 'PICCST067',
  'EXCEPT_NAME' VALUE 'Transaksi Repetitive Biosolar Tidak Wajar',
  'REGIONAL' VALUE z.REGIONAL,
  'SBM' VALUE z.SBM,
  'SAM' VALUE z.SAM,
  'OBJECT_DETAIL' VALUE JSON_OBJECT(
  'SPBU' VALUE b.site_id,
  'PROVINSI' VALUE z.propinsi,
  'GRADE_NAME_STD' VALUE b.grade_name_std,
  'TRANSACTION_VOLUME' VALUE b.delivery_volume,
  'TRANSACTION_VALUE' VALUE b.delivery_value,
  'COUNT_SIGNAL' VALUE b.jlh
  )
  ) DATA_DETAIL
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
                  b.*
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
                                      trunc(b.completed_ts)= trunc(sysdate)-1 
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
  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id;
 COMMIT;

--PICCST068(done)
SELECT 
  trunc(b.completed_ts) AS date_ts,
  JSON_OBJECT(
  'SUB_HOLDING' VALUE 'COMMERCIAL & TRADING',
  'USE_CASE' VALUE 'INOVASI',
  'SUB_USE_CASE' VALUE 'SPBU',
  'EXCEPT_CODE' VALUE 'PICCST068',
  'EXCEPT_NAME' VALUE 'Tera Tidak Wajar',
  'REGIONAL' VALUE z.REGIONAL,
  'SBM' VALUE z.SBM,
  'SAM' VALUE z.SAM,
  'OBJECT_DETAIL' VALUE JSON_OBJECT(
  'SPBU' VALUE b.site_id,
  'PROVINSI' VALUE z.propinsi,
  'GRADE_NAME_STD' VALUE b.grade_name_std,
  'TRANSACTION_VOLUME' VALUE b.delivery_volume,
  'TRANSACTION_VALUE' VALUE b.delivery_value,
  'COUNT_SIGNAL' VALUE b.jumlah
  )
  ) DATA_DETAIL 
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
          trunc(b.completed_ts) = trunc(sysdate) -1 
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
  JOIN APP_IEM_INOVASI.MASTER_SPBU_SBM_SAM z ON b.site_id = z.site_id;
 COMMIT;
  
--PICCST064(done)
SELECT 
  trunc(b.completed_ts) AS date_ts,
  JSON_OBJECT(
  'SUB_HOLDING' VALUE 'COMMERCIAL & TRADING',
  'USE_CASE' VALUE 'INOVASI',
  'SUB_USE_CASE' VALUE 'SPBU',
  'EXCEPT_CODE' VALUE 'PICCST064',
  'EXCEPT_NAME' VALUE 'Transaksi rekomendasi di daerah tidak wajar',
  'REGIONAL' VALUE z.REGIONAL,
  'SBM' VALUE z.SBM,
  'SAM' VALUE z.SAM,
  'OBJECT_DETAIL' VALUE JSON_OBJECT(
  'SPBU' VALUE b.site_id,
  'PROVINSI' VALUE z.propinsi,
  'GRADE_NAME_STD' VALUE b.grade_name_std,
  'TRANSACTION_VOLUME' VALUE sum(b.delivery_volume),
  'TRANSACTION_VALUE' VALUE sum(b.delivery_value),
  'COUNT_NIK' VALUE count(b.agency_name)
  )
  ) DATA_DETAIL 
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
          ) as kotas
        from 
          (
            select 
              b.*
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
                      trunc(b.COMPLETED_TS) = trunc(sysdate)-1 
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
  z.SAM,
  b.site_id,
  z.propinsi,
  b.grade_name_std,
  b.completed_ts;
 COMMIT;

--PICCST070
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
                  trunc(b.COMPLETED_TS)= trunc(sysdate)-1 
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
      trunc(b.COMPLETED_TS) as completed_ts
    from 
      (
        select 
          b.*, 
          dense_rank() over(
            partition by b.SITE_ID 
            order by 
              b.DELIVERY_VOLUME desc, 
              b.COMPLETED_TS desc
          ) as drank
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
                  trunc(b.COMPLETED_TS)= trunc(sysdate)-1 
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

--CNTAMT001
select 
  'INOVASI' USE_CASE, 
  'SPBU' SUB_USE_CASE, 
  'CNTAMT001' EXCEPT_CODE, 
  'Working loss harian di SPBU, untuk melihat kegiatan transaksi minyak di SPBU (PERTALITE)' EXCEPT_NAME, 
  'NOMOR SPBU' "OBJECT", 
  c.REGIONAL, 
  c.SBM, 
  c.SAM, 
  TRUNC(
    max(b.transaction_date)
  ) as date_ts, 
  max(grade_name_std) as grade_name_std, 
  sum(b.volume_diff * 1000) as delivery_volume, 
  sum(b.volume_diff * 1000 * 180) as delivery_value, 
  count(
    DISTINCT trim(b.site_id)
  ) as jumlah 
from 
  (
    select 
      b.*, 
      case when DELIVERY_VOLUME > 0 then abs(
        (
          (DELIVERY_VOLUME - penjualan)/(DELIVERY_VOLUME)
        )* 100
      ) else 0 end as PERSENT 
    from 
      (
        select 
          b.transaction_date, 
          b.site_id, 
          b.grade_name_std, 
          to_number(
            REPLACE(
              REPLACE(b.stok_awal, ','), 
              '.', 
              ','
            )
          ) stok_awal, 
          to_number(
            REPLACE(
              REPLACE(b.penerimaan, ','), 
              '.', 
              ','
            )
          ) penerimaan_totalisator, 
          a.total_do, 
          to_number(
            REPLACE(
              REPLACE(b.penjualan_real, ','), 
              '.', 
              ','
            )
          ) penjualan_real, 
          to_number(
            REPLACE(
              REPLACE(b.stok_akhir_real, ','), 
              '.', 
              ','
            )
          ) stok_akhir_real, 
          (
            to_number(
              REPLACE(
                REPLACE(b.stok_awal, ','), 
                '.', 
                ','
              )
            )+ to_number(
              REPLACE(
                REPLACE(b.penerimaan, ','), 
                '.', 
                ','
              )
            )
          ) - (
            to_number(
              REPLACE(
                REPLACE(b.stok_akhir_real, ','), 
                '.', 
                ','
              )
            )+ to_number(
              REPLACE(
                REPLACE(b.penjualan_real, ','), 
                '.', 
                ','
              )
            )
          ) as volume_diff, 
          to_number(
            REPLACE(
              REPLACE(
                b.percent_gain_or_loss_diatas_toleransi, 
                ','
              ), 
              '.', 
              ','
            )
          ) percent_gain_or_loss_diatas_toleransi, 
          b.volume_diatas_toleransi, 
          DELIVERY_VOLUME, 
          to_number(
            REPLACE(
              REPLACE(b.penjualan_real, ','), 
              '.', 
              ','
            )
          ) * 1000 as penjualan 
        from 
          (
            select 
              distinct site_id, 
              COMPLETED_TS, 
              t.DELIVERY_VOLUME, 
              t.DELIVERY_VOLUME / 1000 as total_do, 
              case when t.GRADE_NAME_STD = 'BIO_SOLAR' then 'BIO SOLAR' else t.GRADE_NAME_STD end as produk 
            from 
              vw_fact_cnt_sales_detail_2023 t 
            where 
              t.GRADE_NAME_STD = 'PERTALITE' 
              and t.DELIVERY_VOLUME > 0
          ) a 
          left join IEM_TBL_IEDCC_LAPORAN_TOTALISATOR_10 b on a.COMPLETED_TS = b.transaction_date 
          and a.site_id = b.site_id 
          and a.produk = b.grade_name_std 
        where 
          trunc(a.COMPLETED_TS)= trunc(sysdate)-10 
          and a.produk in ('PERTALITE')
      ) b 
    where 
      1 = 1 
      and volume_diff >= 4
  ) b 
  left join MASTER_SPBU_SBM_SAM c on b.site_id = c.site_id 
where 
  REGEXP_LIKE(b.site_id, '^[0-9]+$') 
  and persent <= 5 
GROUP BY 
  c.REGIONAL, 
  c.SBM, 
  c.SAM;
COMMIT;
  
--CNTAMT002
select 
b.date_ts,
  JSON_OBJECT(
  'SUB_HOLDING' VALUE 'COMMERCIAL & TRADING',
  'USE_CASE' VALUE 'INOVASI',
  'SUB_USE_CASE' VALUE 'SPBU',
  'EXCEPT_CODE' VALUE 'PICCST067',
  'EXCEPT_NAME' VALUE 'Transaksi Repetitive Biosolar Tidak Wajar',
  'REGIONAL' VALUE z.REGIONAL,
  'SBM' VALUE z.SBM,
  'SAM' VALUE z.SAM,
  'OBJECT_DETAIL' VALUE JSON_OBJECT(
  'SPBU' VALUE b.site_id,
  'PROVINSI' VALUE z.propinsi,
  'GRADE_NAME_STD' VALUE b.grade_name_std,
  'TRANSACTION_VOLUME' VALUE b.delivery_volume,
  'TRANSACTION_VALUE' VALUE b.delivery_value,
  'COUNT_SIGNAL' VALUE b.jlh
  )
  ) DATA_DETAIL
  'INOVASI' USE_CASE, 
  'SPBU' SUB_USE_CASE, 
  'CNTAMT002' EXCEPT_CODE, 
  'Working loss harian di SPBU, untuk melihat kegiatan transaksi minyak di SPBU (BIO SOLAR)' EXCEPT_NAME, 
  'NOMOR SPBU' "OBJECT", 
  c.REGIONAL, 
  c.SBM, 
  c.SAM, 
  TRUNC(
    max(b.transaction_date)
  ) as date_ts, 
  max(grade_name_std) as grade_name_std, 
  sum(b.volume_diff * 1000) as delivery_volume, 
  sum(b.volume_diff * 1000 * 4257) as delivery_value, 
  count(
    DISTINCT trim(b.site_id)
  ) as jumlah 
from 
  (
    select 
      b.*, 
      case when DELIVERY_VOLUME > 0 then abs(
        (
          (DELIVERY_VOLUME - penjualan)/(DELIVERY_VOLUME)
        )* 100
      ) else 0 end as PERSENT 
    from 
      (
        select 
          b.transaction_date, 
          b.site_id, 
          b.grade_name_std, 
          to_number(
            REPLACE(
              REPLACE(b.stok_awal, ','), 
              '.', 
              ','
            )
          ) stok_awal, 
          to_number(
            REPLACE(
              REPLACE(b.penerimaan, ','), 
              '.', 
              ','
            )
          ) penerimaan_totalisator, 
          a.total_do, 
          to_number(
            REPLACE(
              REPLACE(b.penjualan_real, ','), 
              '.', 
              ','
            )
          ) penjualan_real, 
          to_number(
            REPLACE(
              REPLACE(b.stok_akhir_real, ','), 
              '.', 
              ','
            )
          ) stok_akhir_real, 
          (
            to_number(
              REPLACE(
                REPLACE(b.stok_awal, ','), 
                '.', 
                ','
              )
            )+ to_number(
              REPLACE(
                REPLACE(b.penerimaan, ','), 
                '.', 
                ','
              )
            )
          ) - (
            to_number(
              REPLACE(
                REPLACE(b.stok_akhir_real, ','), 
                '.', 
                ','
              )
            )+ to_number(
              REPLACE(
                REPLACE(b.penjualan_real, ','), 
                '.', 
                ','
              )
            )
          ) as volume_diff, 
          to_number(
            REPLACE(
              REPLACE(
                b.percent_gain_or_loss_diatas_toleransi, 
                ','
              ), 
              '.', 
              ','
            )
          ) percent_gain_or_loss_diatas_toleransi, 
          b.volume_diatas_toleransi, 
          DELIVERY_VOLUME, 
          to_number(
            REPLACE(
              REPLACE(b.penjualan_real, ','), 
              '.', 
              ','
            )
          ) * 1000 as penjualan 
        from 
          (
            select 
              distinct site_id, 
              COMPLETED_TS, 
              t.DELIVERY_VOLUME, 
              t.DELIVERY_VOLUME / 1000 as total_do, 
              case when t.GRADE_NAME_STD = 'BIO_SOLAR' then 'BIO SOLAR' else t.GRADE_NAME_STD end as produk 
            from 
              vw_fact_cnt_sales_detail_2023 t 
            where 
              t.GRADE_NAME_STD = 'PERTALITE' 
              and t.DELIVERY_VOLUME > 0
          ) a 
          left join IEM_TBL_IEDCC_LAPORAN_TOTALISATOR_10 b on a.COMPLETED_TS = b.transaction_date 
          and a.site_id = b.site_id 
          and a.produk = b.grade_name_std 
        where 
          trunc(a.COMPLETED_TS)= trunc(sysdate)-10 
          and a.produk in ('BIO SOLAR')
      ) b 
    where 
      1 = 1 
      and volume_diff >= 4
  ) b 
  left join MASTER_SPBU_SBM_SAM c on b.site_id = c.site_id 
where 
  REGEXP_LIKE(b.site_id, '^[0-9]+$') 
  and persent <= 5 
GROUP BY 
  c.REGIONAL, 
  c.SBM, 
  c.SAM;
COMMIT;

--PICCST057
select 
  'INOVASI' USE_CASE, 
  'SPBU' SUB_USE_CASE, 
  'PICCST057' EXCEPT_CODE, 
  'Transaksi Biosolar berulang dengan waktu berdekatan di SPBU untuk No Pol sama di hari yang sama' EXCEPT_NAME, 
  'NOMOR SPBU' "OBJECT", 
  c.REGIONAL, 
  c.SBM, 
  c.SAM, 
  TRUNC(
    max(b.date_ts)
  ) as date_ts, 
  max(grade_name_std) as grade_name_std, 
  sum(b.delivery_volume) as delivery_volume, 
  sum(b.delivery_value) as delivery_value, 
  count(
    DISTINCT trim(c.site_id)
  ) as jumlah 
from 
  (
    select 
      b.DATE_TS, 
      b.SITE_ID, 
      b.GRADE_NAME_STD, 
      min(b.MIN_COMPLETED_TS) as MIN_COMPLETED_TS, 
      max(b.COMPLETED_TS) as COMPLETED_TS, 
      listagg(b.VEHICLE_NUMBER, ',') within group (
        order by 
          b.VEHICLE_NUMBER
      ) as VEHICLE_NUMBER, 
      sum(b.DELIVERY_VOLUME) as DELIVERY_VOLUME, 
      sum(b.DELIVERY_VALUE) as DELIVERY_VALUE, 
      sum(b.JLH) as jlh, 
      sum(b.jlh_vehicle) jlh_vehicle 
    from 
      (
        select 
          trunc(b.tgl_transaksi) as date_ts, 
          min(b.tgl_transaksi) as min_completed_ts, 
          max(b.tgl_transaksi) as completed_ts, 
          trim(b.vehicle_number) as vehicle_number, 
          b.site_id, 
          b.grade_name_std, 
          sum(b.delivery_volume) as delivery_volume, 
          sum(b.delivery_value) as delivery_value, 
          sum(jlh) as jlh, 
          COUNT(
            DISTINCT trim(b.vehicle_number)
          ) jlh_vehicle 
        from 
          (
            select 
              * 
            from 
              (
                select 
                  trunc(b.COMPLETED_TS) as tgl_transaksi, 
                  b.site_id, 
                  trim(b.VEHICLE_NUMBER) as vehicle_number, 
                  min(menit) as menit, 
                  b.grade_name_std, 
                  sum(b.delivery_volume) as delivery_volume, 
                  sum(b.delivery_value) as delivery_value, 
                  count(*) as jlh 
                from 
                  (
                    select 
                      b.*, 
                      (COMPLETED_TS - bf_r) * 24 * 60 as menit 
                    from 
                      (
                        select 
                          b.*, 
                          nvl(
                            lag(b.COMPLETED_TS) over(
                              partition by b.SITE_ID, 
                              trim(b.VEHICLE_NUMBER) 
                              order by 
                                b.COMPLETED_TS
                            ), 
                            sysdate - 2
                          ) as bf_r 
                        from 
                          iem_vw_iedcc_det_catatan_transaksi b 
                        where 
                          trunc(b.COMPLETED_TS) = trunc(sysdate)-1 
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
                          and trim(b.vehicle_number) not like 'TN%PB' 
                          and trim(b.vehicle_number) not like 'PS%'
                      ) b 
                    where 
                      1 = 1
                  ) b 
                where 
                  trim(b.VEHICLE_NUMBER) is not null 
                group by 
                  trunc(b.COMPLETED_TS), 
                  b.site_id, 
                  trim(b.VEHICLE_NUMBER), 
                  b.grade_name_std
              ) a 
            where 
              menit <= 15
          ) b 
        group by 
          trunc(b.tgl_transaksi), 
          trim(b.vehicle_number), 
          b.grade_name_std, 
          b.SITE_ID
      ) b 
    where 
      b.DELIVERY_VOLUME >= 60.1 
    group by 
      b.DATE_TS, 
      b.SITE_ID, 
      b.GRADE_NAME_STD 
    HAVING 
      sum(b.jlh_vehicle) >= 5
  ) b 
  join MASTER_SPBU_SBM_SAM c on b.site_id = c.site_id 
GROUP BY 
  c.REGIONAL, 
  c.SBM, 
  c.SAM;
COMMIT;

--PICCST072
select 
  'INOVASI' USE_CASE, 
  'SPBU' SUB_USE_CASE, 
  'PICCST072' EXCEPT_CODE, 
  'Transaksi Biosolar menggunakan NIK/ Surat Rekomendasi (over kuota)' EXCEPT_NAME, 
  'NOMOR SPBU' "OBJECT", 
  c.REGIONAL, 
  c.SBM, 
  c.SAM, 
  TRUNC(
    max(b.tanggal_transaksi_terakhir)
  ) as date_ts, 
  max(grade_name_std) as grade_name_std, 
  sum(b.volume_over_kuota) as delivery_volume, 
  sum(
    (
      b.total_value_actual / b.total_volume_actual
    )* b.volume_over_kuota
  ) as delivery_value, 
  count(
    DISTINCT trim(c.site_id)
  ) as jumlah 
from 
  (
    WITH data_model AS (
      SELECT 
        a.completed_ts, 
        a.site_id, 
        a.regional, 
        a.alamat, 
        a.kota, 
        a.propinsi, 
        a.tipe_agen, 
        a.grade_name_std, 
        a.agency_name, 
        a.surkom, 
        CASE WHEN a.completed_ts BETWEEN b.tanggal_mulai 
        AND b.tanggal_berakhir THEN b.tanggal_mulai ELSE NULL END tanggal_mulai, 
        CASE WHEN a.completed_ts BETWEEN b.tanggal_mulai 
        AND b.tanggal_berakhir THEN b.tanggal_berakhir ELSE NULL END tanggal_berakhir, 
        CASE WHEN a.completed_ts BETWEEN b.tanggal_mulai 
        AND b.tanggal_berakhir THEN b.total_kuota ELSE 0 END total_kuota, 
        a.delivery_volume, 
        a.delivery_value, 
        a.jumlah_transaksi 
      FROM 
        IEM_TBL_IEDCC_TRANSAKSI_BY_NIK_REV a 
        LEFT JOIN (
          SELECT 
            DISTINCT nik, 
            surkom, 
            total_kuota, 
            tanggal_mulai, 
            tanggal_berakhir 
          FROM 
            IEM_MVW_REGISTRATION_NON_VEHICLE 
          WHERE 
            tanggal_berakhir >= TO_DATE('2022-12-01', 'YYYY-MM-DD')
        ) b ON a.agency_name = b.nik 
        AND a.surkom = TRIM(
          UPPER(b.surkom)
        )
    ), 
    data_ AS (
      SELECT 
        agency_name, 
        surkom, 
        site_id, 
        regional, 
        alamat, 
        kota, 
        propinsi, 
        tipe_agen, 
        grade_name_std, 
        tanggal_mulai, 
        tanggal_berakhir, 
        max(total_kuota) max_kuota, 
        sum(delivery_volume) total_volume_actual, 
        sum(delivery_value) total_value_actual, 
        sum(jumlah_transaksi) jumlah_transaksi_actual, 
        sum(delivery_volume)- MAX(total_kuota) volume_over_kuota, 
        MAX(completed_ts) tanggal_transaksi_terakhir -- TO_CHAR(MIN(completed_ts),'YYYY-Mon-DD')||' s/d '||TO_CHAR(MAX(completed_ts),'YYYY-Mon-DD') periode_transaksi, 
      FROM 
        data_model 
      WHERE 
        tanggal_mulai IS NOT NULL 
      GROUP BY 
        agency_name, 
        surkom, 
        site_id, 
        grade_name_std, 
        tanggal_mulai, 
        tanggal_berakhir, 
        regional, 
        alamat, 
        kota, 
        propinsi, 
        tipe_agen
    ) 
    SELECT 
      DISTINCT * 
    FROM 
      data_ 
    WHERE 
      volume_over_kuota > 0 
      AND tanggal_transaksi_terakhir = TRUNC(SYSDATE)-1 
      and grade_name_std = 'BIO_SOLAR'
  ) b 
  join MASTER_SPBU_SBM_SAM c on b.site_id = c.site_id 
GROUP BY 
  c.REGIONAL, 
  c.SBM, 
  c.SAM;
COMMIT;

--CNTAMT007
SELECT s.date_report date_ts,
  JSON_OBJECT(
  'SUB_HOLDING' VALUE 'COMMERCIAL & TRADING',
  'USE_CASE' VALUE 'INOVASI',
  'SUB_USE_CASE' VALUE 'TBBM',
  'EXCEPT_CODE' VALUE 'CNTAMT007',
  'EXCEPT_NAME' VALUE 'Realisasi MS2 Harian < 90%',
  'FLAG' VALUE 'Revenue Enhancement',
  'REGIONAL' VALUE z.REGIONAL,
  'SBM' VALUE z.SBM,
  'SAM' VALUE z.SAM,
  'OBJECT_DETAIL' VALUE JSON_OBJECT(
  'SPBU' VALUE b.site_id,
  'PROVINSI' VALUE z.propinsi,
  'GRADE_NAME_STD' VALUE b.grade_name_std,
  'TRANSACTION_VOLUME' VALUE b.delivery_volume,
  'TRANSACTION_VALUE' VALUE b.delivery_value,
  'COUNT_SIGNAL' VALUE b.jlh
  )
  ) DATA_DETAIL
select s.date_report,s.plant_code,s.produk,b.provinsi, s.KL_PLANNING_HOHI,s.KL_PLANNING_HMIN1,KL_PLANNING_OUSTANDING,KL_REALISASI_TOTAL,s.realisasi_ms2 from ( select s.date_report,s.tbbm,s.plant_code,
             CASE WHEN PRODUK IN ('BIOSOLAR B35', 'BIOSOLAR B30') THEN n'BIOSOLAR' 
WHEN PRODUK = 'PERTAMAX TURBO, BULK' THEN n'PERTAMAX TURBO' WHEN PRODUK = 'PERTAMAX,BULK' THEN n'PERTAMAX' 
WHEN PRODUK = 'PERTAMINA DEX 50 PPM, BULK' THEN n'PERTAMINA DEX' ELSE PRODUK END PRODUK,s.realisasi_ms2,
             --round(((s.kl_realisasi_sesuai_ritase+kl_realisasi_terlambat_ritase)/(s.kl_planning_hmin1 + kl_planning_oustanding - kl_realisasi_mendahului_hari))*100,2) as realisasi_ms2,
             s.kinerja_ms2,
             s.kl_planning_hmin1,
             s.KL_PLANNING_HOHI,
             s.kl_planning_oustanding,
             s.kl_realisasi_mendahului_hari,
             s.kl_planning_hmin1 + kl_planning_oustanding - s.kl_realisasi_mendahului_hari kl_planning_total,
             s.kl_realisasi_sesuai_ritase,
             s.kl_realisasi_mendahului_ritase,
             s.kl_realisasi_terlambat_ritase,
             s.kl_realisasi_oustanding,
             s.kl_realisasi_sesuai_ritase+s.kl_realisasi_mendahului_ritase+kl_realisasi_terlambat_ritase-kl_realisasi_oustanding as kl_realisasi_total2,
             s.kl_realisasi_hohi,
             s.kl_realisasi_hplus1,
             s.kl_realisasi_hohi + kl_realisasi_hplus1 as kl_realisasi_total,
             (s.kl_realisasi_sesuai_ritase+s.kl_realisasi_mendahului_ritase+kl_realisasi_terlambat_ritase-kl_realisasi_oustanding) +
              (s.kl_realisasi_hohi + kl_realisasi_hplus1) as grant_total,
             s.last_update,s.realisasi_ms2 as ms2_realisasi
        from app_iem_inovasi.IEM_TBL_DISTRIBUTION_PERFORMANCE s
        where kl_planning_hmin1 > 0 
        and produk <> 'PERTAMAX GREEN 95') s
LEFT JOIN         
(SELECT * FROM IEM_TBL_IEDCC_TBBM a
JOIN DATA_ASSET_PERTAMINA b ON a.ID_TBBM = b.ID_ASSET) z ON s.plant_code = z.id_tbbm

