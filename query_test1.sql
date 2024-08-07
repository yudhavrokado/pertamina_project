USE PERTAMINADIMENSIONAL;
GO

ALTER VIEW iedcc.spreading_index_iedcc_version
AS
WITH manipulated
AS (
	SELECT a.formatted_pstng_date
		,b.plant
		,b.product
	FROM [PertaminaDimensional].[dbo].[FACT_LOGISTIC_OPT_SPREADING_INDEX] a
	CROSS JOIN (
		SELECT DISTINCT plant
			,product
		FROM [PertaminaDimensional].[dbo].[FACT_LOGISTIC_OPT_SPREADING_INDEX]
		) b
	)
	,tabel_base
AS (
	SELECT DISTINCT a.formatted_pstng_date
		,a.plant
		,a.product
		,CASE 
			WHEN a.formatted_pstng_date = '2022-11-01'
				AND b.total_stock IS NULL
				THEN coalesce(b.total_stock, 0)
			ELSE b.total_stock
			END total_stock
		,CASE 
			WHEN a.formatted_pstng_date = '2022-11-01'
				AND b.safety_stock IS NULL
				THEN coalesce(b.safety_stock, 0)
			ELSE b.safety_stock
			END safety_stock
		,CASE 
			WHEN a.formatted_pstng_date = '2022-11-01'
				AND b.cycle_stock IS NULL
				THEN coalesce(b.cycle_stock, 0)
			ELSE b.cycle_stock
			END cycle_stock
		,CASE 
			WHEN a.formatted_pstng_date = '2022-11-01'
				AND b.spreading_index IS NULL
				THEN coalesce(b.spreading_index, 0)
			ELSE b.spreading_index
			END spreading_index
	FROM manipulated a
	LEFT JOIN [PertaminaDimensional].[dbo].[FACT_LOGISTIC_OPT_SPREADING_INDEX] b ON a.formatted_pstng_date = b.formatted_pstng_date
		AND a.plant = b.plant
		AND a.product = b.product
	)
	,final_tabel
AS (
	SELECT formatted_pstng_date
		,plant
		,product
		,total_stock
		,count(total_stock) OVER (
			PARTITION BY plant
			,product ORDER BY formatted_pstng_date ROWS UNBOUNDED PRECEDING
			) total_stock_grp
		,safety_stock
		,count(safety_stock) OVER (
			PARTITION BY plant
			,product ORDER BY formatted_pstng_date ROWS UNBOUNDED PRECEDING
			) safety_stock_grp
		,cycle_stock
		,count(cycle_stock) OVER (
			PARTITION BY plant
			,product ORDER BY formatted_pstng_date ROWS UNBOUNDED PRECEDING
			) cycle_stock_grp
		,spreading_index
		,count(spreading_index) OVER (
			PARTITION BY plant
			,product ORDER BY formatted_pstng_date ROWS UNBOUNDED PRECEDING
			) spreading_index_grp
	FROM tabel_base
	)
SELECT formatted_pstng_date
	,plant
	,product
	,total_stock
	,coalesce(total_stock, FIRST_VALUE(total_stock) OVER (
			PARTITION BY plant
			,product
			,total_stock_grp ORDER BY formatted_pstng_date
			)) total_stock_new
	,safety_stock
	,coalesce(safety_stock, FIRST_VALUE(safety_stock) OVER (
			PARTITION BY plant
			,product
			,safety_stock_grp ORDER BY formatted_pstng_date
			)) safety_stock_new
	,cycle_stock
	,coalesce(cycle_stock, FIRST_VALUE(cycle_stock) OVER (
			PARTITION BY plant
			,product
			,cycle_stock_grp ORDER BY formatted_pstng_date
			)) cycle_stock_new
	,spreading_index
	,coalesce(spreading_index, FIRST_VALUE(spreading_index) OVER (
			PARTITION BY plant
			,product
			,spreading_index_grp ORDER BY formatted_pstng_date
			)) spreading_index_new
FROM final_tabel;