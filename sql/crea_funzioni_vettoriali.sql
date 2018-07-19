--19 giugno 2018
-- estrae dai centroidi prossimi alle centraline di pm10 i parametri necessari per la stima del modello
CREATE OR REPLACE FUNCTION vgriglia.get_vector_value() RETURNS 
TABLE ( 
	id integer,
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
	co_punt double precision,
	cod_reg smallint
) AS $$ 
BEGIN
	RETURN QUERY
	
	WITH gr AS (
		-- seleziona le centraline che hanno dati di pm10 nel 2015, ovvero le centraline che compaiono nella vista pm10_con_dati
		WITH staz AS
		(
			SELECT 
				id_centralina,
				idcell,
				geom 
			FROM 
				centraline.pm10_con_dati
		)
		-- identifica le maglie della griglia che contengono le centraline di pm10 con dati nel 2015
		SELECT 
			griglia.geom 
		FROM
			staz,vgriglia.griglia
		WHERE
			St_Contains(griglia.geom,staz.geom)
	)
	-- interseca le maglie della griglia sopra identificate con i centroidi e ricava i valori per i parametri desiderati
	SELECT 
		idcell::integer,
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
		co_puntual::double precision,
		regioni::smallint
	FROM
		vgriglia.centroidi,gr
	WHERE
		St_intersects(gr.geom,centroidi.geom);

END;$$
LANGUAGE plpgsql;
