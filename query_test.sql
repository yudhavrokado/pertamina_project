alter session set NLS_NUMERIC_CHARACTERS = '.,';
with data_detail_mt as (select b.plateno,
                               x.nipsupir,
                               x.supir,
                               x.produk,
                               b.latitude,
                               b.longitude,
                               a.nama_asset,
                               a.latitude  as latitude_tbbm,
                               a.longitude as longitude_tbbm,
                               (6371 * ACOS(
                                       COS(TO_NUMBER(b.latitude) * (3.141592653589793 / 180)) *
                                       COS(TO_NUMBER(a.latitude) * (3.141592653589793 / 180)) *
                                       COS((TO_NUMBER(a.longitude) - TO_NUMBER(b.longitude)) *
                                           (3.141592653589793 / 180)) +
                                       SIN(TO_NUMBER(b.latitude) * (3.141592653589793 / 180)) *
                                       SIN(TO_NUMBER(a.latitude) * (3.141592653589793 / 180))
                                       ))  AS distance_to_tbbm_km,
                               b.insert_date,
                               x.spbu,
                               x.extrainfo,
                               x.latitude  as latitude_spbu,
                               x.longitude as longitude_spbu,
                               (6371 * ACOS(
                                       COS(TO_NUMBER(b.latitude) * (3.141592653589793 / 180)) *
                                       COS(TO_NUMBER(x.latitude) * (3.141592653589793 / 180)) *
                                       COS((TO_NUMBER(x.longitude) - TO_NUMBER(b.longitude)) *
                                           (3.141592653589793 / 180)) +
                                       SIN(TO_NUMBER(b.latitude) * (3.141592653589793 / 180)) *
                                       SIN(TO_NUMBER(x.latitude) * (3.141592653589793 / 180))
                                       ))  AS distance_to_spbu_km,
                               LAG(b.insert_date)
                                              OVER (PARTITION BY b.plateno, b.latitude, b.longitude, x.spbu order by b.insert_date) as prev_insert_date
                        from APP_IEM_INOVASI.IEM_TBL_IEDCC_MT_GPS_LAST_LOCATION b
                                 left join (select *
                                            from APP_IEM_INOVASI.DATA_ASSET_PERTAMINA
                                            where jenis_asset = 'TBBM') a on b.fleet_id = a.id_asset
                                 left join (select distinct p.mt,
                                                            p.plantcode,
                                                            SUBSTR(p.gateouttime, 1, 10) as gateouttime,
                                                            p.spbu,
                                                            p.produk,
                                                            p.nipsupir,
                                                            p.supir,
                                                            p.extrainfo,
                                                            q.latitude,
                                                            q.longitude
                                            from APP_IEM_INOVASI.IEM_TBL_IEDCC_MT_SHIPMENT_GATE_OUT p
                                                     left join (select distinct site_id, latitude, longitude
                                                                from APP_IEM_INOVASI.IEM_TBL_IEDCC_SPBU_PROFILE) q
                                                               ON p.SPBU = q.site_id
                                            where regexp_like(gateouttime, '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
                                              and trunc(to_date(gateouttime, 'YYYY-MM-DD HH24:MI:SS')) >=
                                                  trunc(sysdate) - 60) x
                                           ON
                                               b.plateno = x.mt
                                                   and to_char(b.insert_date, 'YYYY-MM-DD') = x.gateouttime
                                                   and b.fleet_id = x.plantcode
                        where trunc(b.insert_date) >= trunc(sysdate) - 60
                          and b.speed = 0
                          and regexp_like(b.latitude, '^-?\d+(\.\d+)?$')
                          and regexp_like(b.longitude, '^-?\d+(\.\d+)?$')
                          and to_number(b.latitude) between -11.0 AND 6.0
                          and to_number(b.longitude) between 95.0 AND 141.0),
     signal_mt_30_menit_stop as (select r.*,
                                        case
                                            when r.prev_insert_date is not null
                                                then (r.insert_date - r.prev_insert_date) * 1440
                                            else 0
                                            end as lama_berhenti
                                 from data_detail_mt r
                                 where r.distance_to_spbu_km > 1
                                   and r.distance_to_tbbm_km > 2)
select plateno,
       nipsupir,
       supir,
       produk,
       latitude                     as lat_mt,
       longitude                    as long_mt,
       nama_asset                   as tbbm_asal,
       latitude_tbbm                as lat_tbbm,
       longitude_tbbm               as long_tbbm,
       distance_to_tbbm_km,
       spbu                         as spbu_tujuan,
       latitude_spbu                as lat_spbu,
       longitude_spbu               as long_spbu,
       distance_to_spbu_km,
       extrainfo,
       min(insert_date)             as start_time,
       max(insert_date)             as end_time,
       round(sum(lama_berhenti), 2) as lama_berhenti,
       count(*)                     as jumlah_titik_berhenti
from signal_mt_30_menit_stop
GROUP BY plateno, nipsupir, supir, produk, latitude, longitude, spbu, extrainfo, nama_asset, latitude_tbbm,
         longitude_tbbm, latitude_spbu,
         longitude_spbu, distance_to_spbu_km, distance_to_tbbm_km
HAVING sum(lama_berhenti) >= 30
ORDER BY plateno, start_time;