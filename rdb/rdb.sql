-- 确认rdb中dn和cn的情况：
SELECT cluster_id,db_ip,db_port,db_status FROM mds.db_info WHERE db_ip = ?;
SELECT cluster_id,proxy_ip,proxy_port,proxy_status FROM mds.proxy_info a LEFT JOIN mds.cluster_proxy_bind_info b ON a.proxy_id=b.proxy_id WHERE proxy_ip = ?;

SELECT device_ip,device_port,apply_user FROM goldendb_omm.gdb_device_info WHERE device_ip = ?;

SELECT i.id,i.host_ip,i.db_port,i.user_name FROM goldendb_insight.insight_install_db_info i WHERE i.host_ip = ?;
SELECT r.install_id,r.host_ip,r.db_name FROM goldendb_insight.insight_tenancy_db_resource r WHERE r.host_ip = ?;
SELECT i.id,i.host_ip,i.listen_port,i.user_name FROM goldendb_insight.insight_install_proxy_info i WHERE i.host_ip = ?;
SELECT r.install_id,r.host_ip,r.proxy_name FROM goldendb_insight.insight_tenancy_proxy_resource r WHERE r.host_ip = ?;