--Problema: il database postgis è venuto quando già si era svolto parte del lavoro lavorando sullo shapefile dei centroidi
--a cui sono stati assegnati i valori dei vari rasters e vettoriali. Questi valori assegnati sono corretti? In corso d'opera si è commesso qualche errore?
-- Sospetto: la funzione di R raster::extract ha un'opzione che permette di mantenere i campi NA...il sospetto è che in qualche estrazione non si sia utilizzato
--correttamente extract.

--La procedura che seque crea una tabella vgriglia.sintesi_stats utilizzando la funzione vgriglia.randomPoints in cui ai valori dei centroidi sono confrontati
--con gli stessi valori acquisiti on-the-fly mediante postgis dai vari campi vettoriali e rasters. Queste differenze tra valori sono tutte pari a zero?

BEGIN;
DROP TABLE IF EXISTS vgriglia.puntiCasuali;
--DROP TABLE IF EXISTS casuali;

CREATE TEMPORARY TABLE casuali AS (

		SELECT * FROM vgriglia.randomPoints(15000)

);

CREATE INDEX casuali_idx ON casuali USING gist(geometria);


CREATE TABLE vgriglia.puntiCasuali AS(

	--estrae punti casuali da vgriglia.centroidi	

	--Ai valori del corine land cover associamo ora il valore 0, ovvero non li inseriamo nella query. Perchè?
	-- perchè i vettori (poligoni) del corine land cover non sono spazialmente contigui..questo probabilmente comporta che, se inseriti in questa query,
        --postgis non riesce a sfruttare l'indice spaziale e la query diventa interminabile. Invece il successivo aggiornamento dei campi del corine landcover
        -- (gli update che seguono) sono operazioni velocissime.
	SELECT
		c.*,
		round(St_Value(dem.rast,1,c.geometria)) AS rdem,
		round(St_Value(dis_a1.rast,1,c.geometria)) AS rdis_a1,
		round(St_Value(dis_a2.rast,1,c.geometria)) AS rdis_a2,
		round(St_Value(dis_aero.rast,1,c.geometria)) AS rdis_aero,
		round(St_Value(dis_costa.rast,1,c.geometria)) AS rdis_costa,
		round(St_Value(impervious.rast,1,c.geometria)) AS rimpervious,
		clzone.zona AS vclzone,
		round(St_Value(popistat.rast,1,c.geometria)) AS rpopistat, 
		0 AS vclc_agri,
		0 AS vclc_arbl,
		0 AS vclc_crop,
		0 AS vclc_dcds,
		0 AS vclc_evgr,
		0 AS vclc_hidv,
		0 AS vclc_lwdv,
		0 AS vclc_pstr,
		0 AS vclc_shrb,
		emissioni.pm10_diffu as vpm10_diffu,
		emissioni.nh3_diffus as vnh3_diffu,
		emissioni.co_puntual as vco_puntual,
		St_Value(avg_buf_a1.rast,1,c.geometria) AS ravg_buf_a1, 
		St_Value(avg_buf_a23.rast,1,c.geometria) AS ravg_buf_a23, 
		St_Value(avg_buf_oth.rast,1,c.geometria) AS ravg_buf_oth

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
		vgriglia.emissioni emissioni,
		rgriglia.avg_buf_a1,
		rgriglia.avg_buf_a23,
		rgriglia.avg_buf_oth

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
			St_Intersects(emissioni.geom,c.geometria) AND
			St_Intersects(avg_buf_a1.rast,1,c.geometria) AND	
			St_Intersects(avg_buf_a23.rast,1,c.geometria) AND	
			St_Intersects(avg_buf_oth.rast,1,c.geometria)

		)

);


CREATE INDEX puntiCasuali_idx ON vgriglia.puntiCasuali USING gist(geometria);

-- a questo punto acquisiamo i valori dai vettoriali del Corine Land Cover.
UPDATE vgriglia.puntiCasuali SET vclc_agri=z.perc FROM (

	Select area/10000 AS perc,geom FROM vgriglia.clc_agri  
	

) z WHERE St_intersects(vgriglia.puntiCasuali.geometria,z.geom);


UPDATE vgriglia.puntiCasuali SET vclc_arbl=z.perc FROM (

	Select area/10000 AS perc,geom FROM vgriglia.clc_arbl  
	

) z WHERE St_intersects(vgriglia.puntiCasuali.geometria,z.geom);



UPDATE vgriglia.puntiCasuali SET vclc_dcds=z.perc FROM (

	Select area/10000 AS perc,geom FROM vgriglia.clc_dcds  
	

) z WHERE St_intersects(vgriglia.puntiCasuali.geometria,z.geom);


UPDATE vgriglia.puntiCasuali SET vclc_crop=z.perc FROM (

	Select area/10000 AS perc,geom FROM vgriglia.clc_crop  
	

) z WHERE St_intersects(vgriglia.puntiCasuali.geometria,z.geom);



UPDATE vgriglia.puntiCasuali SET vclc_hidv=z.perc FROM (

	Select area/10000 AS perc,geom FROM vgriglia.clc_hidv 
	

) z WHERE St_intersects(vgriglia.puntiCasuali.geometria,z.geom);


UPDATE vgriglia.puntiCasuali SET vclc_lwdv=z.perc FROM (

	Select area/10000 AS perc,geom FROM vgriglia.clc_lwdv  
	

) z WHERE St_intersects(vgriglia.puntiCasuali.geometria,z.geom);



UPDATE vgriglia.puntiCasuali SET vclc_evgr=z.perc FROM (

	Select area/10000 AS perc,geom FROM vgriglia.clc_evgr 
	

) z WHERE St_intersects(vgriglia.puntiCasuali.geometria,z.geom);


UPDATE vgriglia.puntiCasuali SET vclc_pstr=z.perc FROM (

	Select area/10000 AS perc,geom FROM vgriglia.clc_pstr  
	

) z WHERE St_intersects(vgriglia.puntiCasuali.geometria,z.geom);


UPDATE vgriglia.puntiCasuali SET vclc_shrb=z.perc FROM (

	Select area/10000 AS perc,geom FROM vgriglia.clc_shrb 
	

) z WHERE St_intersects(vgriglia.puntiCasuali.geometria,z.geom);

DROP TABLE IF EXISTS vgriglia.sintesi;

--tabella con le differenze tra i campi già assegnati ai centroidi e gli stessi campi riacquisiti dai raster
CREATE TABLE vgriglia.sintesi AS (

	SELECT 
	id,
	geometria,
	d_impianti,
	d_a1-rdis_a1 AS diff_dis_a1,
	d_a2-rdis_a2 AS diff_dis_a2,
	d_aero-rdis_aero AS diff_dis_aero,
	i_surface-rimpervious AS diff_i_surface,
	d_costa-rdis_costa AS diff_d_costa,
	q_dem - rdem AS diff_q_dem,
	climate_zone-vclzone AS diff_climate_zone,
	p_istat-rpopistat AS diff_p_istat,
	cl_agri-vclc_agri AS diff_cl_agri,
	cl_arbl-vclc_arbl AS diff_cl_arbl,
	cl_crop-vclc_crop AS diff_cl_crop,
	cl_dcds-vclc_dcds AS diff_cl_dcds,
	cl_evgr-vclc_evgr AS diff_cl_evgr,
	cl_hidv-vclc_hidv AS diff_cl_hidv,
	cl_lwdv -vclc_lwdv AS diff_cl_lwdv,
	cl_pstr -vclc_pstr AS diff_cl_pstr,
	cl_shrb-vclc_shrb AS diff_cl_shrb,
	av_buf_a1-ravg_buf_a1 AS diff_avg_buf_a1,
	av_buf_a23-ravg_buf_a23 AS diff_avg_buf_a23,
	av_buf_oth-ravg_buf_oth AS diff_avg_buf_oth,
	pm10_diff-vpm10_diffu AS diff_pm10_diffu, 
	nh3_diff - vnh3_diffu AS diff_nh3_diffu,
	co_punt- vco_puntual AS diff_co_punt

	FROM

		vgriglia.puntiCasuali



);

--tabella in cui, per ogni centroide tra quelli casualmente estratti, compare il valore minimo e massimo delle differenze
-- tra campi assegnati ai centroidi e i campi ricalcolati dai rasters
DROP TABLE IF EXISTS vgriglia.sintesi_stats;

CREATE TABLE vgriglia.sintesi_stats AS (

	SELECT 
	id,
	geometria,
	MIN(diff_dis_a1) AS min_diff_dis_a1,
	MAX(diff_dis_a1) AS max_diff_dis_a1,
	MIN(diff_dis_a2) AS min_diff_dis_a2,
	MAX(diff_dis_a2) AS max_diff_dis_a2,
	MIN(diff_dis_aero) AS min_diff_dis_aero,
	MAX(diff_dis_aero) AS max_diff_dis_aero,
	MIN(diff_i_surface) AS min_diff_i_surface,
	MAX(diff_i_surface) AS max_diff_i_surface,
	MIN(diff_d_costa) AS min_diff_d_costa,
	MAX(diff_d_costa) AS max_diff_d_costa,
	MIN(diff_q_dem) AS min_diff_q_dem,
	MAX(diff_q_dem) AS max_diff_q_dem,
	MIN(diff_climate_zone) AS min_diff_climate_zone,
	MAX(diff_climate_zone) AS max_diff_climate_zone,	
	MIN(diff_p_istat) AS min_diff_p_istat,
	MAX(diff_p_istat) AS max_diff_p_istat,	
	MIN(diff_cl_agri) AS min_diff_cl_agri,
	MAX(diff_cl_agri) AS max_diff_cl_agri,	
	MIN(diff_cl_arbl) AS min_diff_cl_arbl,
	MAX(diff_cl_arbl) AS max_diff_cl_arbl,	
	MIN(diff_cl_crop) AS min_diff_cl_crop,
	MAX(diff_cl_crop) AS max_diff_cl_crop,	
	MIN(diff_cl_dcds) AS min_diff_cl_dcds,
	MAX(diff_cl_dcds) AS max_diff_cl_dcds,	
	MIN(diff_cl_evgr) AS min_diff_cl_evgr,
	MAX(diff_cl_evgr) AS max_diff_cl_evgr,	
	MIN(diff_cl_hidv) AS min_diff_cl_hidv,
	MAX(diff_cl_hidv) AS max_diff_cl_hidv,	
	MIN(diff_cl_lwdv) AS min_diff_cl_lwdv,
	MAX(diff_cl_lwdv) AS max_diff_cl_lwdv,	
	MIN(diff_cl_pstr) AS min_diff_cl_pstr,
	MAX(diff_cl_pstr) AS max_diff_cl_pstr,	
	MIN(diff_cl_shrb) AS min_diff_cl_shrb,
	MAX(diff_cl_shrb) AS max_diff_cl_shrb,	
	MIN(diff_avg_buf_a1) AS min_diff_avg_buf_a1,
	MAX(diff_avg_buf_a1) AS max_diff_avg_buf_a1,
	MIN(diff_avg_buf_a23) AS min_diff_avg_buf_a23,
	MAX(diff_avg_buf_a23) AS max_diff_avg_buf_a23,
	MIN(diff_avg_buf_oth) AS min_diff_avg_buf_oth,
	MAX(diff_avg_buf_oth) AS max_diff_avg_buf_oth,
	MIN(diff_pm10_diffu) AS min_diff_pm10_diffu,
	MAX(diff_pm10_diffu) AS max_diff_pm10_diffu,
	MIN(diff_nh3_diffu) AS min_diff_nh3_diffu,
	MAX(diff_nh3_diffu) AS max_diff_nh3_diffu,
	MIN(diff_co_punt) AS min_diff_co_punt,
	MAX(diff_co_punt) AS max_diff_co_punt

	FROM

		vgriglia.sintesi
	GROUP BY
		id,geometria

);

CREATE INDEX stats_idx ON vgriglia.sintesi_stats USING gist(geometria);

COMMIT;
