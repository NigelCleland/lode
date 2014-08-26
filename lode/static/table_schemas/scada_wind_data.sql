CREATE TABLE IF NOT EXISTS scada_wind_data_%s
(
scada_wind_data_key serial primary key,
Stamp Timestamp,
bpe_statn_generation_twf_mw double precision,
ltn_statn_generation_twf_mw double precision,
twc_statn_generation_twc_mw double precision,
twh_statn_generation_tuk_mw double precision,
trh_statn_generation_trh_mw double precision,
hwb_statn_generation_mah_mw double precision,
wdv_statn_generation_tap_mw double precision,
wwd_statn_generate_wwd_a_mw double precision,
wwd_statn_generate_wwd_b_mw double precision,
nma_statn_generation_whl_mw double precision,
UNIQUE(Stamp)
);
