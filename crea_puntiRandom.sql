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
