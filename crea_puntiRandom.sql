-- funzione per estrarre punti casuali da vgriglia.centroidi
CREATE OR REPLACE FUNCTION vgriglia.randomPoints(numeroPunti integer)
RETURNS TABLE(

	id integer,
	geometria geometry,
	d_impianti integer,
	d_a1 integer,
	d_a2 integer,
	d_aero integer,
	i_surface integer,
	d_costa integer,
	q_dem integer,
	climate_zone integer,
	p_istat integer,
	cl_agri integer,
	cl_arbl integer,
	cl_crop integer,
	cl_dcds integer,
	cl_evgr integer,
	cl_hidv integer,
	cl_lwdv integer,
	cl_pstr integer,
	cl_shrb integer,
	av_buf_a1 double precision,
	av_buf_a23 double precision,
	av_buf_oth double precision,
	pm10_diff double precision,
	nh3_diff double precision,
	co_punt double precision

) AS $$
BEGIN
	
	RETURN QUERY

	SELECT 

		idcell::integer,
		geom,
		dis_imp::integer,
		dis_a1::integer,
		dis_a2::integer,
		dis_aero::integer,
		impervious::integer,
		dis_costa::integer,
		dem::integer,
		clzone::integer,
		popistat::integer,
		clc_agri::integer,
		clc_arbl::integer,
		clc_crop::integer,
		clc_dcds::integer,
		clc_evgr::integer,
		clc_hidv::integer,
		clc_lwdv::integer,
		clc_pstr::integer,
		clc_shrb::integer,
		avg_buf_a1::double precision,
		avg_buf_a23::double precision,
		avg_buf_oth::double precision,
		pm10_diffu::double precision,
		nh3_diffu::double precision,
		co_puntual::double precision
	FROM

		vgriglia.centroidi

	WHERE
		idcell IN (
		
			SELECT 
				floor(random()*307635 +1)::integer AS idcell

			FROM

				generate_series(1,numeroPunti)

	);
END;$$
LANGUAGE plpgsql;



DROP TABLE IF EXISTS vgriglia.puntiCasuali;


CREATE TABLE vgriglia.puntiCasuali AS(

	--estrae punti casuali da vgriglia.centroidi	
	WITH casuali AS (

		SELECT * FROM vgriglia.randomPoints(1000)

	)
	--crea una tabella in cui ai valori dei centroidi sono associati i valori 
	--estratti dai raster in modo di confrontare i valori memorizzati su centroidi
	--con quelli presi da raster. Se tutti i programmi in R sono corretti i valori 
	--debbono coincidere
	SELECT

		c.*,
		St_Value(dem.rast,1,c.geometria) AS rdem,
		St_Value(dis_a1.rast,1,c.geometria) AS rdis_a1,
		St_Value(dis_a2.rast,1,c.geometria) AS rdis_a2,
		St_Value(dis_aero.rast,1,c.geometria) AS rdis_aero,
		St_Value(dis_costa.rast,1,c.geometria) AS rdis_costa,
		St_Value(impervious.rast,1,c.geometria) AS rimpervious,
		clzone.zona AS vclzone,
		St_Value(popistat.rast,1,c.geometria) AS rpopistat, 
		CASE
			WHEN St_Intersects(clc_agri.geom,c.geometria) THEN
				clc_agri.area
			ELSE
				0
		END AS vclc_agri,
		CASE
			WHEN St_Intersects(clc_arbl.geom,c.geometria) THEN
				clc_arbl.area
			ELSE
				0
		END AS vclc_arbl,
		CASE
			WHEN St_Intersects(clc_crop.geom,c.geometria) THEN
				clc_crop.area
			ELSE
				0
		END AS vclc_crop,
		CASE
			WHEN St_Intersects(clc_evgr.geom,c.geometria) THEN
				clc_evgr.area
			ELSE
				0
		END AS vclc_evgr,
		CASE
			WHEN St_Intersects(clc_dcds.geom,c.geometria) THEN
				clc_dcds.area
			ELSE
				0
		END AS vclc_dcds,
		CASE
			WHEN St_Intersects(clc_hidv.geom,c.geometria) THEN
				clc_hidv.area
			ELSE
				0
		END AS vclc_hidv,
		CASE
			WHEN St_Intersects(clc_lwdv.geom,c.geometria) THEN
				clc_lwdv.area
			ELSE
				0
		END AS vclc_lwdv,
		CASE
			WHEN St_Intersects(clc_shrb.geom,c.geometria) THEN
				clc_shrb.area
			ELSE
				0
		END AS vclc_shrb,
		CASE
			WHEN St_Intersects(clc_pstr.geom,c.geometria) THEN
				clc_pstr.area
			ELSE
				0
		END AS vclc_pstr,
		emissioni.pm10_diffu as vpm10_diffu,
		emissioni.nh3_diffus as vnh3_diffu,
		emissioni.co_puntual as vco_puntual 


	FROM
		casuali c,
		rgriglia.dem dem,
		rgriglia.dis_a1 dis_a1,
		rgriglia.dis_a2 dis_a2,
		rgriglia.dis_aero dis_aero,
		rgriglia.dis_costa dis_costa,
		rgriglia.impervious impervious,
		vgriglia.clzone clzone,
		rgriglia.popistat popistat,
		vgriglia.clc_agri clc_agri,
		vgriglia.clc_arbl clc_arbl,
		vgriglia.clc_crop clc_crop,
		vgriglia.clc_evgr clc_evgr,
		vgriglia.clc_hidv clc_hidv,
		vgriglia.clc_lwdv clc_lwdv,
		vgriglia.clc_pstr clc_pstr,
		vgriglia.clc_shrb clc_shrb,
		vgriglia.clc_dcds clc_dcds,
		vgriglia.emissioni emissioni

	WHERE
		(
			St_Intersects(dem.rast,1,c.geometria) AND	
			St_Intersects(dis_a1.rast,1,c.geometria) AND	
			St_Intersects(dis_a2.rast,1,c.geometria) AND	
			St_Intersects(dis_aero.rast,1,c.geometria) AND	
			St_Intersects(dis_costa.rast,1,c.geometria) AND
			St_Intersects(impervious.rast,1,c.geometria) AND
			St_Intersects(clzone.geom,c.geometria) AND
			St_Intersects(popistat.rast,c.geometria) AND
			(St_Intersects(clc_agri.geom,c.geometria) OR
			St_Intersects(clc_arbl.geom,c.geometria) OR
			St_Intersects(clc_crop.geom,c.geometria) OR
		        St_Intersects(clc_dcds.geom,c.geometria) OR			      				St_Intersects(clc_evgr.geom,c.geometria) OR
			St_Intersects(clc_hidv.geom,c.geometria) OR
		        St_Intersects(clc_lwdv.geom,c.geometria) OR			      				St_Intersects(clc_pstr.geom,c.geometria) OR
			St_Intersects(clc_shrb.geom,c.geometria)) AND
			St_Intersects(emissioni.geom,c.geometria)



		)

);















