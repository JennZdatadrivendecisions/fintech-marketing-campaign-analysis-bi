CREATE DATABASE IF NOT EXISTS lufax;
# 读取数据
USE lufax;

SELECT 
    table_name AS '表名', 
    table_rows AS '估算行数 (近似值)', 
    CONCAT(ROUND(data_length / 1024 / 1024, 2), ' MB') AS '数据大小'
FROM information_schema.tables 
WHERE table_schema = 'lufax'
ORDER BY table_rows DESC;

# 缺失字段
SET SQL_SAFE_UPDATES = 0;

UPDATE dim_user du
JOIN fact_registration fr ON du.user_id = fr.user_id
SET 
    du.is_card_binded = 1,  
    du.card_bind_time = DATE_ADD(fr.reg_time, INTERVAL FLOOR(5 + (RAND() * 55)) MINUTE) -- 绑卡时间 = 注册时间 + 随机几分钟
WHERE 
    fr.reg_time >= '2023-11-04 00:00:00' 
    AND RAND() < 0.3; 
SET SQL_SAFE_UPDATES = 1;

# 用户维度表 ETL
CREATE VIEW v_dim_user AS
SELECT 
	*,
    COALESCE(user_id, CONCAT('Guest_', device_id)) AS User_ID_Key
FROM 
    dim_user;

# 流量表ETL
DROP TABLE IF EXISTS fact_traffic_detail;
CREATE TABLE fact_traffic_detail AS
SELECT 
	*,
    CASE 
        WHEN t.action_detail LIKE '%:%' THEN SUBSTRING_INDEX(t.action_detail, ':', -1)
        ELSE NULL 
    END AS resource_id,
    TIMESTAMPDIFF(SECOND, t.event_time,t.next_event_time) AS stay_duration_second
FROM(
	SELECT 
        *,
        LEAD(event_time) OVER (PARTITION BY COALESCE(user_id, device_id) ORDER BY event_time ASC) AS next_event_time
    FROM fact_traffic
    ) t;

# 订单表ETL
ALTER TABLE fact_order
ADD COLUMN resource_type varchar(100);
UPDATE fact_order
SET resource_type =
	CASE 
		WHEN redpacket_id IS NOT NULL AND coupon_id IS NOT NULL THEN '混合使用'
        WHEN redpacket_id IS NOT NULL THEN '现金红包'
        WHEN coupon_id IS NOT NULL THEN '卡券抵扣'
	ELSE '未使用优惠'
    END;


ALTER TABLE fact_order ADD COLUMN Marketing_cost DECIMAL(18, 5);
UPDATE fact_order
SET Marketing_cost = 
	COALESCE(redpacket_reward, 0) + 
    (
     order_amount * COALESCE(coupon_yield_rate, 0) * (investment_term_days / 365.0)
     );

# 新客全链路转化漏斗
DROP view IF EXISTS  v_new_user_funnel;
CREATE VIEW v_new_user_funnel AS
SELECT 
	COUNT(DISTINCT CASE WHEN fd.popup_type = 'new_user_gift' THEN fd.device_id ELSE NULL END) AS '新客',
    COUNT(DISTINCT CASE
		WHEN fr.reg_time >= '2023-11-04 00:00:00' THEN fr.user_id ELSE NULL END) AS '新客注册',
    COUNT(DISTINCT CASE 
        WHEN vd.card_bind_time >= '2023-11-04 00:00:00' THEN fd.user_id ELSE NULL END) AS '新客绑卡人数',
    COUNT(DISTINCT CASE 
		WHEN fo.order_time >= '2023-11-04 00:00:00' AND fr.reg_time >= '2023-11-04 00:00:00' THEN fo.user_id 
        ELSE NULL END) AS '首投转化人数'
FROM fact_traffic_detail fd
LEFT JOIN fact_registration fr ON fd.device_id = fr.device_id
LEFT JOIN v_dim_user vd ON fd.user_id = vd.User_ID_Key
LEFT JOIN fact_order fo ON fd.user_id = fo.user_id
WHERE fd.event_time >= '2023-11-04 00:00:00';


# 老客复投转化链路
CREATE VIEW v_funnel_old_customer AS
# 领取游戏奖励老客
WITH game_prize AS (
	SELECT 
		fg.user_id
	FROM 
		fact_game fg
	JOIN v_dim_user vd
	ON vd.User_ID_Key = fg.user_id
	WHERE 
		fg.game_type = 'lottery' 
		AND fg.resource_id IS NOT NULL
		AND vd.user_type != 'new'
),
# 领取弹窗奖励用户
popup_prize AS(
	SELECT
		fd.user_id
	FROM 
		fact_traffic_detail  fd
	JOIN v_dim_user vd
	ON vd.User_ID_Key = fd.user_id
	WHERE 
		fd.event_type = 'resource_grant' 
        AND fd.resource_id IS NOT NULL
		AND vd.user_type != 'new'
)


SELECT 
    '1. 参与活动' AS stage_name, 
    1 AS stage_order,
    COUNT(DISTINCT t1.user_id) AS user_count
FROM (
    (SELECT fd.user_id 
    FROM 
		fact_traffic_detail  fd
	JOIN v_dim_user vd
	ON vd.User_ID_Key = fd.user_id 
    WHERE vd.user_type != 'new')
    UNION (
    SELECT fg.user_id 
	FROM fact_game fg
	JOIN v_dim_user vd ON vd.User_ID_Key = fg.user_id
    WHERE vd.user_type != 'new')
) t1
UNION ALL 
SELECT
	'2. 成功领奖' AS stage_name, 
    2 AS stage_order,
	COUNT(DISTINCT pp.user_id) AS '领奖老客'
FROM
	popup_prize PP
JOIN
	game_prize gp
ON gp.user_id = pp.user_id
UNION ALL
SELECT 
	'3. 复投转化' AS stage_name, 
    3 AS stage_order,
     COUNT(DISTINCT fo.user_id)  AS '复投用户'
FROM fact_order fo
JOIN v_dim_user vd
ON vd.User_ID_Key = fo.user_id
WHERE fo.is_first_investment = 0 AND fo.order_status = 'completed';


# 集福卡漏斗
CREATE VIEW v_cardcollect_funnel AS 
WITH card_collect AS(
SELECT
	user_id,
	COUNT(DISTINCT card_name) AS card_num
FROM
	fact_game
WHERE
	game_type IN ( 'card_exchange', 'card_collect',  'card_synthesis')
    AND
    task_status IN ('completed', 'received', 'success')
GROUP BY user_id
)

SELECT
	COUNT(DISTINCT user_id) AS '总参与人数',
    COUNT(DISTINCT CASE WHEN card_num >=1 THEN  user_id ELSE NULL END) AS '参与集卡用户数',
    COUNT(DISTINCT CASE WHEN card_num >=4 THEN  user_id ELSE NULL END) AS '集齐普通卡用户数',
    COUNT(DISTINCT CASE WHEN card_num >=5 THEN  user_id ELSE NULL END) AS '集齐5张用户数',
	COUNT(DISTINCT CASE WHEN card_num >= 6 THEN user_id ELSE NULL END) AS '集齐6张用户数'
FROM card_collect;

# 邀请拉新
CREATE VIEW v_cardinvite AS 
WITH game_invite AS(
SELECT
	user_id,
	COUNT(DISTINCT related_user_id) AS invite_count
FROM fact_game
WHERE game_type = 'invite' AND task_status = 'completed'
GROUP BY user_id
)

SELECT 
	CASE 
        WHEN invite_count >= 5 THEN '5人及以上 (通关大神)'
        WHEN invite_count = 1 THEN '1人 (刚试水)'
        ELSE CONCAT(invite_count, '人') 
    END AS invite_bucket,
    COUNT(user_id) AS user_count
FROM game_invite
GROUP BY 
    CASE 
        WHEN invite_count >= 5 THEN '5人及以上 (通关大神)'
        WHEN invite_count = 1 THEN '1人 (刚试水)'
        ELSE CONCAT(invite_count, '人') 
    END;

# 原因：大量用户求稀有卡说明游戏参与度较高，有很大一部分完成邀请任务但是没得到最后一张稀有卡的用户，掉落概率是固定的，游戏规则需要动态调整


# RFM
CREATE VIEW v_RFM AS
SELECT
	du.user_id,
    du.city,         
    du.risk_level,    
    du.age,
    TIMESTAMPDIFF(DAY, du.last_invest_time, MAX(fo.order_time)) AS '沉睡唤醒周期',
    COUNT(fo.order_id) AS '活动投资频率(次)',
	SUM(fo.order_amount) AS '活动投资总金额',
    du.total_aum AS '历史AUM'
FROM fact_order fo
LEFT JOIN dim_user du
ON du.user_id = fo.user_id
GROUP BY du.user_id, du.city, du.risk_level, du.age, du.last_invest_time, du.total_aum;

SELECT DISTINCT event_type FROM fact_traffic_detail
    
