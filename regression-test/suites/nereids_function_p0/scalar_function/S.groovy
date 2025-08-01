// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("nereids_scalar_fn_S") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_second_DateTime "select second(kdtm) from fn_test order by kdtm"
	qt_sql_second_DateTime_notnull "select second(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_second_DateTimeV2 "select second(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_second_DateTimeV2_notnull "select second(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_second_DateV2 "select second(kdtv2) from fn_test order by kdtv2"
	qt_sql_second_DateV2_notnull "select second(kdtv2) from fn_test_not_nullable order by kdtv2"
	qt_sql_second_ceil_DateTime "select second_ceil(kdtm) from fn_test order by kdtm"
	qt_sql_second_ceil_DateTime_notnull "select second_ceil(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_second_ceil_DateTimeV2 "select second_ceil(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_second_ceil_DateTimeV2_notnull "select second_ceil(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_second_ceil_DateTime_DateTime "select second_ceil(kdtm, kdtm) from fn_test order by kdtm, kdtm"
	qt_sql_second_ceil_DateTime_DateTime_notnull "select second_ceil(kdtm, kdtm) from fn_test_not_nullable order by kdtm, kdtm"
	qt_sql_second_ceil_DateTime_Integer "select second_ceil(kdtm, kint) from fn_test order by kdtm, kint"
	qt_sql_second_ceil_DateTime_Integer_notnull "select second_ceil(kdtm, kint) from fn_test_not_nullable order by kdtm, kint"
	qt_sql_second_ceil_DateTimeV2_DateTimeV2 "select second_ceil(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
	qt_sql_second_ceil_DateTimeV2_DateTimeV2_notnull "select second_ceil(kdtmv2s1, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kdtmv2s1"
	qt_sql_second_ceil_DateTimeV2_Integer "select second_ceil(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
	qt_sql_second_ceil_DateTimeV2_Integer_notnull "select second_ceil(kdtmv2s1, kint) from fn_test_not_nullable order by kdtmv2s1, kint"
	qt_sql_second_ceil_DateTime_Integer_DateTime "select second_ceil(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
	qt_sql_second_ceil_DateTime_Integer_DateTime_notnull "select second_ceil(kdtm, kint, kdtm) from fn_test_not_nullable order by kdtm, kint, kdtm"
	qt_sql_second_ceil_DateTimeV2_Integer_DateTimeV2 "select second_ceil(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
	qt_sql_second_ceil_DateTimeV2_Integer_DateTimeV2_notnull "select second_ceil(kdtmv2s1, kint, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kint, kdtmv2s1"
	qt_sql_second_floor_DateTime "select second_floor(kdtm) from fn_test order by kdtm"
	qt_sql_second_floor_DateTime_notnull "select second_floor(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_second_floor_DateTimeV2 "select second_floor(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_second_floor_DateTimeV2_notnull "select second_floor(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_second_floor_DateTime_DateTime "select second_floor(kdtm, kdtm) from fn_test order by kdtm, kdtm"
	qt_sql_second_floor_DateTime_DateTime_notnull "select second_floor(kdtm, kdtm) from fn_test_not_nullable order by kdtm, kdtm"
	qt_sql_second_floor_DateTime_Integer "select second_floor(kdtm, kint) from fn_test order by kdtm, kint"
	qt_sql_second_floor_DateTime_Integer_notnull "select second_floor(kdtm, kint) from fn_test_not_nullable order by kdtm, kint"
	qt_sql_second_floor_DateTimeV2_DateTimeV2 "select second_floor(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
	qt_sql_second_floor_DateTimeV2_DateTimeV2_notnull "select second_floor(kdtmv2s1, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kdtmv2s1"
	qt_sql_second_floor_DateTimeV2_Integer "select second_floor(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
	qt_sql_second_floor_DateTimeV2_Integer_notnull "select second_floor(kdtmv2s1, kint) from fn_test_not_nullable order by kdtmv2s1, kint"
	qt_sql_second_floor_DateTime_Integer_DateTime "select second_floor(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
	qt_sql_second_floor_DateTime_Integer_DateTime_notnull "select second_floor(kdtm, kint, kdtm) from fn_test_not_nullable order by kdtm, kint, kdtm"
	qt_sql_second_floor_DateTimeV2_Integer_DateTimeV2 "select second_floor(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
	qt_sql_second_floor_DateTimeV2_Integer_DateTimeV2_notnull "select second_floor(kdtmv2s1, kint, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kint, kdtmv2s1"
	qt_sql_seconds_add_DateTime_Integer "select seconds_add(kdtm, kint) from fn_test order by kdtm, kint"
	qt_sql_seconds_add_DateTime_Integer_notnull "select seconds_add(kdtm, kint) from fn_test_not_nullable order by kdtm, kint"
	qt_sql_seconds_add_DateTimeV2_Integer "select seconds_add(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
	qt_sql_seconds_add_DateTimeV2_Integer_notnull "select seconds_add(kdtmv2s1, kint) from fn_test_not_nullable order by kdtmv2s1, kint"
	qt_sql_seconds_add_Date_Integer "select seconds_add(kdt, kint) from fn_test order by kdt, kint"
	qt_sql_seconds_add_Date_Integer_notnull "select seconds_add(kdt, kint) from fn_test_not_nullable order by kdt, kint"
	qt_sql_seconds_add_DateV2_Integer "select seconds_add(kdtv2, kint) from fn_test order by kdtv2, kint"
	qt_sql_seconds_add_DateV2_Integer_notnull "select seconds_add(kdtv2, kint) from fn_test_not_nullable order by kdtv2, kint"
	qt_sql_seconds_diff_DateTime_DateTime "select seconds_diff(kdtm, kdtm) from fn_test order by kdtm, kdtm"
	qt_sql_seconds_diff_DateTime_DateTime_notnull "select seconds_diff(kdtm, kdtm) from fn_test_not_nullable order by kdtm, kdtm"
	qt_sql_seconds_diff_DateTimeV2_DateTimeV2 "select seconds_diff(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
	qt_sql_seconds_diff_DateTimeV2_DateTimeV2_notnull "select seconds_diff(kdtmv2s1, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kdtmv2s1"
	qt_sql_seconds_diff_DateV2_DateTimeV2 "select seconds_diff(kdtv2, kdtmv2s1) from fn_test order by kdtv2, kdtmv2s1"
	qt_sql_seconds_diff_DateV2_DateTimeV2_notnull "select seconds_diff(kdtv2, kdtmv2s1) from fn_test_not_nullable order by kdtv2, kdtmv2s1"
	qt_sql_seconds_diff_DateTimeV2_DateV2 "select seconds_diff(kdtmv2s1, kdtv2) from fn_test order by kdtmv2s1, kdtv2"
	qt_sql_seconds_diff_DateTimeV2_DateV2_notnull "select seconds_diff(kdtmv2s1, kdtv2) from fn_test_not_nullable order by kdtmv2s1, kdtv2"
	qt_sql_seconds_diff_DateV2_DateV2 "select seconds_diff(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
	qt_sql_seconds_diff_DateV2_DateV2_notnull "select seconds_diff(kdtv2, kdtv2) from fn_test_not_nullable order by kdtv2, kdtv2"
	qt_sql_seconds_diff_DateV2_DateTime "select seconds_diff(kdtv2, kdtm) from fn_test order by kdtv2, kdtm"
	qt_sql_seconds_diff_DateV2_DateTime_notnull "select seconds_diff(kdtv2, kdtm) from fn_test_not_nullable order by kdtv2, kdtm"
	qt_sql_seconds_diff_DateTime_DateV2 "select seconds_diff(kdtm, kdtv2) from fn_test order by kdtm, kdtv2"
	qt_sql_seconds_diff_DateTime_DateV2_notnull "select seconds_diff(kdtm, kdtv2) from fn_test_not_nullable order by kdtm, kdtv2"
	qt_sql_seconds_diff_DateTimeV2_DateTime "select seconds_diff(kdtmv2s1, kdtm) from fn_test order by kdtmv2s1, kdtm"
	qt_sql_seconds_diff_DateTimeV2_DateTime_notnull "select seconds_diff(kdtmv2s1, kdtm) from fn_test_not_nullable order by kdtmv2s1, kdtm"
	qt_sql_seconds_diff_DateTime_DateTimeV2 "select seconds_diff(kdtm, kdtmv2s1) from fn_test order by kdtm, kdtmv2s1"
	qt_sql_seconds_diff_DateTime_DateTimeV2_notnull "select seconds_diff(kdtm, kdtmv2s1) from fn_test_not_nullable order by kdtm, kdtmv2s1"
	qt_sql_sign_Double "select sign(kdbl) from fn_test order by kdbl"
	qt_sql_sign_Double_notnull "select sign(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_signbit_Double "select signbit(kdbl) from fn_test order by kdbl"
	qt_sql_signbit_Double_notnull "select signbit(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_sin_Double "select sin(kdbl) from fn_test order by kdbl"
	qt_sql_sin_Double_notnull "select sin(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_sleep_Integer "select sleep(0.1) from fn_test order by kint"
	qt_sql_sleep_Integer_notnull "select sleep(0.1) from fn_test_not_nullable order by kint"
	qt_sql_sm3_Varchar "select sm3(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_sm3_Varchar_notnull "select sm3(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_sm3_String "select sm3(kstr) from fn_test order by kstr"
	qt_sql_sm3_String_notnull "select sm3(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_sm3sum_Varchar "select sm3sum(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_sm3sum_Varchar_notnull "select sm3sum(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_sm3sum_String "select sm3sum(kstr) from fn_test order by kstr"
	qt_sql_sm3sum_String_notnull "select sm3sum(kstr) from fn_test_not_nullable order by kstr"
	
	sql "select sm4_decrypt(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	sql "select sm4_decrypt(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	sql "select sm4_decrypt(kstr, kstr) from fn_test order by kstr, kstr"
	sql "select sm4_decrypt(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	sql "select sm4_decrypt(kvchrs1, kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1, kvchrs1"
	sql "select sm4_decrypt(kvchrs1, kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1, kvchrs1"
	sql "select sm4_decrypt(kstr, kstr, kstr) from fn_test order by kstr, kstr, kstr"
	sql "select sm4_decrypt(kstr, kstr, kstr) from fn_test_not_nullable order by kstr, kstr, kstr"
	sql "select sm4_decrypt(kvchrs1, kvchrs1, kvchrs1, 'SM4_128_ECB') from fn_test order by kvchrs1, kvchrs1, kvchrs1"
	sql "select sm4_decrypt(kvchrs1, kvchrs1, kvchrs1, 'SM4_128_ECB') from fn_test_not_nullable order by kvchrs1, kvchrs1, kvchrs1"
	sql "select sm4_decrypt(kstr, kstr, kstr, 'SM4_128_ECB') from fn_test order by kstr, kstr, kstr"
	sql "select sm4_decrypt(kstr, kstr, kstr, 'SM4_128_ECB') from fn_test_not_nullable order by kstr, kstr, kstr"

	sql "select sm4_encrypt(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	sql "select sm4_encrypt(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	sql "select sm4_encrypt(kstr, kstr) from fn_test order by kstr, kstr"
	sql "select sm4_encrypt(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	sql "select sm4_encrypt(kvchrs1, kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1, kvchrs1"
	sql "select sm4_encrypt(kvchrs1, kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1, kvchrs1"
	sql "select sm4_encrypt(kstr, kstr, kstr) from fn_test order by kstr, kstr, kstr"
	sql "select sm4_encrypt(kstr, kstr, kstr) from fn_test_not_nullable order by kstr, kstr, kstr"
	sql "select sm4_encrypt(kvchrs1, kvchrs1, kvchrs1, 'SM4_128_ECB') from fn_test order by kvchrs1, kvchrs1, kvchrs1"
	sql "select sm4_encrypt(kvchrs1, kvchrs1, kvchrs1, 'SM4_128_ECB') from fn_test_not_nullable order by kvchrs1, kvchrs1, kvchrs1"
	sql "select sm4_encrypt(kstr, kstr, kstr, 'SM4_128_ECB') from fn_test order by kstr, kstr, kstr"
	sql "select sm4_encrypt(kstr, kstr, kstr, 'SM4_128_ECB') from fn_test_not_nullable order by kstr, kstr, kstr"

	sql "select space(10) from fn_test order by kint"
	sql "select space(10) from fn_test_not_nullable order by kint"
	sql """select k from (select length(space(number)) k from numbers("number" = "10"))t;""" // before #44919 will crash
	qt_sql_split_part_Varchar_Varchar_Integer "select split_part(kvchrs1, ' ', 1) from fn_test order by kvchrs1"
	qt_sql_split_part_Varchar_Varchar_Integer_notnull "select split_part(kvchrs1, ' ', 1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_split_part_String_String_Integer "select split_part(kstr, ' ', 1) from fn_test order by kstr"
	qt_sql_split_part_String_String_Integer_notnull "select split_part(kstr, ' ', 1) from fn_test_not_nullable order by kstr"
	qt_sql_sqrt_Double "select sqrt(kdbl) from fn_test order by kdbl"
	qt_sql_sqrt_Double_notnull "select sqrt(kdbl) from fn_test_not_nullable order by kdbl"

	qt_sql_st_astext_Varchar "select st_astext(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_astext_Varchar_notnull "select st_astext(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_astext_String "select st_astext(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_astext_String_notnull "select st_astext(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_aswkt_Varchar "select st_aswkt(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_aswkt_Varchar_notnull "select st_aswkt(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_aswkt_String "select st_aswkt(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_aswkt_String_notnull "select st_aswkt(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_circle_Double_Double_Double "select st_astext(st_circle(x_lng, x_lat, radius)) from fn_test order by 1"
	qt_sql_st_circle_Double_Double_Double_notnull "select st_astext(st_circle(x_lng, x_lat, radius)) from fn_test_not_nullable order by 1"
	qt_sql_st_contains_Varchar_Varchar "select st_contains(st_polygon(polygon_wkt), st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_contains_Varchar_Varchar_notnull "select st_contains(st_polygon(polygon_wkt), st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_distance_sphere_Double_Double_Double_Double "select st_distance_sphere(x_lng, x_lat, y_lng, y_lat) from fn_test order by 1"
	qt_sql_st_distance_sphere_Double_Double_Double_Double_notnull "select st_distance_sphere(x_lng, x_lat, y_lng, y_lat) from fn_test_not_nullable order by 1"
	qt_sql_st_angle_sphere_Double_Double_Double_Double "select st_angle_sphere(x_lng, x_lat, y_lng, y_lat) from fn_test order by 1"
    qt_sql_st_angle_sphere_Double_Double_Double_Double_notnull "select st_angle_sphere(x_lng, x_lat, y_lng, y_lat) from fn_test_not_nullable order by 1"
    qt_sql_st_angle_Varchar_Varchar "select st_angle(st_point(x_lng, x_lat), st_point(y_lng, y_lat), st_point(z_lng, z_lat)) from fn_test order by 1"
    qt_sql_st_angle_Varchar_Varchar_notnull "select st_angle(st_point(x_lng, x_lat), st_point(y_lng, y_lat), st_point(z_lng, z_lat)) from fn_test order by 1"
    qt_sql_st_azimuth_Varchar_Varchar "select st_azimuth(st_point(x_lng, x_lat), st_point(y_lng, y_lat)) from fn_test order by 1"
    qt_sql_st_azimuth_Varchar_Varchar_notnull "select st_azimuth(st_point(x_lng, x_lat), st_point(y_lng, y_lat)) from fn_test order by 1"
    qt_sql_st_area_square_meters_circle "select ST_Area_Square_Meters(ST_Circle(x_lng, x_lat, radius)) from fn_test order by 1"
    qt_sql_st_area_square_meters_circle_notnull "select ST_Area_Square_Meters(ST_Circle(x_lng, x_lat, radius)) from fn_test_not_nullable order by 1"
    qt_sql_st_area_square_meters_polygon "select ST_Area_Square_Meters(st_polygon(polygon_wkt)) from fn_test order by 1"
    qt_sql_st_area_square_meters_polygon_notnull "select ST_Area_Square_Meters(st_polygon(polygon_wkt)) from fn_test_not_nullable order by 1"
    qt_sql_st_area_km_circle "select ST_Area_Square_Km(ST_Circle(x_lng, x_lat, radius)) from fn_test order by 1"
    qt_sql_st_area_km_circle_notnull "select ST_Area_Square_Km(ST_Circle(x_lng, x_lat, radius)) from fn_test_not_nullable order by 1"
    qt_sql_st_area_km_polygon "select ST_Area_Square_Km(st_polygon(polygon_wkt)) from fn_test order by 1"
    qt_sql_st_area_km_polygon_notnull "select ST_Area_Square_Km(st_polygon(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_geometryfromtext_Varchar "select st_astext(st_geometryfromtext(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_geometryfromtext_Varchar_notnull "select st_astext(st_geometryfromtext(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_geometryfromtext_String "select st_astext(st_geometryfromtext(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_geometryfromtext_String_notnull "select st_astext(st_geometryfromtext(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_geomfromtext_Varchar "select st_astext(st_geomfromtext(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_geomfromtext_Varchar_notnull "select st_astext(st_geomfromtext(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_geomfromtext_String "select st_astext(st_geomfromtext(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_geomfromtext_String_notnull "select st_astext(st_geomfromtext(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_linefromtext_Varchar "select st_astext(st_linefromtext(linestring_wkt)) from fn_test order by 1"
	qt_sql_st_linefromtext_Varchar_notnull "select st_astext(st_linefromtext(linestring_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_linefromtext_String "select st_astext(st_linefromtext(linestring_wkt)) from fn_test order by 1"
	qt_sql_st_linefromtext_String_notnull "select st_astext(st_linefromtext(linestring_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_linestringfromtext_Varchar "select ST_AsText(st_linestringfromtext(linestring_wkt)) from fn_test order by 1"
	qt_sql_st_linestringfromtext_Varchar_notnull "select ST_AsText(st_linestringfromtext(linestring_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_linestringfromtext_String "select ST_AsText(st_linestringfromtext(linestring_wkt)) from fn_test order by 1"
	qt_sql_st_linestringfromtext_String_notnull "select ST_AsText(st_linestringfromtext(linestring_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_point_Double_Double "select st_astext(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_point_Double_Double_notnull "select st_astext(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_polyfromtext_Varchar "select ST_AsText(st_polyfromtext(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_polyfromtext_Varchar_notnull "select ST_AsText(st_polyfromtext(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_polyfromtext_String "select ST_AsText(st_polyfromtext(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_polyfromtext_String_notnull "select ST_AsText(st_polyfromtext(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_polygon_Varchar "select ST_AsText(st_polygon(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_polygon_Varchar_notnull "select ST_AsText(st_polygon(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_polygon_String "select ST_AsText(st_polygon(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_polygon_String_notnull "select ST_AsText(st_polygon(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_polygonfromtext_Varchar "select ST_AsText(st_polygonfromtext(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_polygonfromtext_Varchar_notnull "select ST_AsText(st_polygonfromtext(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_polygonfromtext_String "select ST_AsText(st_polygonfromtext(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_polygonfromtext_String_notnull "select ST_AsText(st_polygonfromtext(polygon_wkt)) from fn_test_not_nullable order by 1"
	qt_sql_st_x_Varchar "select st_x(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_x_Varchar_notnull "select st_x(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_x_String "select st_x(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_x_String_notnull "select st_x(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_y_Varchar "select st_y(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_y_Varchar_notnull "select st_y(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_y_String "select st_y(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_y_String_notnull "select st_y(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_asbinary_Varchar "select ST_AsBinary(st_point(x_lng, x_lat)) from fn_test order by 1"
    qt_sql_st_asbinary_Varchar_notnull "select ST_AsBinary(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_geometryfromwkb_Varchar "select ST_AsText(ST_GeometryFromWKB(ST_AsBinary(st_polyfromtext(polygon_wkt)))) from fn_test order by 1"
    qt_sql_st_geometryfromwkb_Varchar_notnull "select ST_AsText(ST_GeometryFromWKB(ST_AsBinary(st_polyfromtext(polygon_wkt)))) from fn_test_not_nullable order by 1"
	qt_sql_st_geomfromwkb_Varchar "select ST_AsText(ST_GeomFromWKB(ST_AsBinary(st_polyfromtext(polygon_wkt)))) from fn_test order by 1"
    qt_sql_st_geomfromwkb_Varchar_notnull "select ST_AsText(ST_GeomFromWKB(ST_AsBinary(st_polyfromtext(polygon_wkt)))) from fn_test_not_nullable order by 1"

	qt_sql_starts_with_Varchar_Varchar "select starts_with(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_starts_with_Varchar_Varchar_notnull "select starts_with(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_starts_with_String_String "select starts_with(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_starts_with_String_String_notnull "select starts_with(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_str_to_date_Varchar_Varchar "select str_to_date(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_str_to_date_Varchar_Varchar_notnull "select str_to_date(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_str_to_date_String_String "select str_to_date(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_str_to_date_String_String_notnull "select str_to_date(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_strleft_Varchar_Integer "select strleft(kvchrs1, kint) from fn_test order by kvchrs1, kint"
	qt_sql_strleft_Varchar_Integer_notnull "select strleft(kvchrs1, kint) from fn_test_not_nullable order by kvchrs1, kint"
	qt_sql_strleft_String_Integer "select strleft(kstr, kint) from fn_test order by kstr, kint"
	qt_sql_strleft_String_Integer_notnull "select strleft(kstr, kint) from fn_test_not_nullable order by kstr, kint"
	qt_sql_strright_Varchar_Integer "select strright(kvchrs1, kint) from fn_test order by kvchrs1, kint"
	qt_sql_strright_Varchar_Integer_notnull "select strright(kvchrs1, kint) from fn_test_not_nullable order by kvchrs1, kint"
	qt_sql_strright_String_Integer "select strright(kstr, kint) from fn_test order by kstr, kint"
	qt_sql_strright_String_Integer_notnull "select strright(kstr, kint) from fn_test_not_nullable order by kstr, kint"
	qt_sql_sub_bitmap_Bitmap_BigInt_BigInt "select sub_bitmap(to_bitmap(kbint), kbint, kbint) from fn_test order by kbint, kbint, kbint"
	qt_sql_sub_bitmap_Bitmap_BigInt_BigInt_notnull "select sub_bitmap(to_bitmap(kbint), kbint, kbint) from fn_test_not_nullable order by kbint, kbint, kbint"
	qt_sql_sub_replace_Varchar_Varchar_Integer "select sub_replace(kvchrs1, kvchrs1, kint) from fn_test order by kvchrs1, kvchrs1, kint"
	qt_sql_sub_replace_Varchar_Varchar_Integer_notnull "select sub_replace(kvchrs1, kvchrs1, kint) from fn_test_not_nullable order by kvchrs1, kvchrs1, kint"
	qt_sql_sub_replace_String_String_Integer "select sub_replace(kstr, kstr, kint) from fn_test order by kstr, kstr, kint"
	qt_sql_sub_replace_String_String_Integer_notnull "select sub_replace(kstr, kstr, kint) from fn_test_not_nullable order by kstr, kstr, kint"
	qt_sql_sub_replace_Varchar_Varchar_Integer_Integer "select sub_replace(kvchrs1, kvchrs1, kint, kint) from fn_test order by kvchrs1, kvchrs1, kint, kint"
	qt_sql_sub_replace_Varchar_Varchar_Integer_Integer_notnull "select sub_replace(kvchrs1, kvchrs1, kint, kint) from fn_test_not_nullable order by kvchrs1, kvchrs1, kint, kint"
	qt_sql_sub_replace_String_String_Integer_Integer "select sub_replace(kstr, kstr, kint, kint) from fn_test order by kstr, kstr, kint, kint"
	qt_sql_sub_replace_String_String_Integer_Integer_notnull "select sub_replace(kstr, kstr, kint, kint) from fn_test_not_nullable order by kstr, kstr, kint, kint"
	qt_sql_substring_Varchar_Integer "select substring(kvchrs1, kint) from fn_test order by kvchrs1, kint"
	qt_sql_substring_Varchar_Integer_notnull "select substring(kvchrs1, kint) from fn_test_not_nullable order by kvchrs1, kint"
	qt_sql_substring_String_Integer "select substring(kstr, kint) from fn_test order by kstr, kint"
	qt_sql_substring_String_Integer_notnull "select substring(kstr, kint) from fn_test_not_nullable order by kstr, kint"
	qt_sql_substring_Varchar_Integer_Integer "select substring(kvchrs1, kint, kint) from fn_test order by kvchrs1, kint, kint"
	qt_sql_substring_Varchar_Integer_Integer_notnull "select substring(kvchrs1, kint, kint) from fn_test_not_nullable order by kvchrs1, kint, kint"
	qt_sql_substring_String_Integer_Integer "select substring(kstr, kint, kint) from fn_test order by kstr, kint, kint"
	qt_sql_substring_String_Integer_Integer_notnull "select substring(kstr, kint, kint) from fn_test_not_nullable order by kstr, kint, kint"
	qt_sql_substring_index_Varchar_Varchar_Integer "select substring_index(kvchrs1, ' ', 2) from fn_test order by kvchrs1"
	qt_sql_substring_index_Varchar_Varchar_Integer_notnull "select substring_index(kvchrs1, ' ', 2) from fn_test_not_nullable order by kvchrs1"
	qt_sql_substring_index_String_String_Integer "select substring_index(kstr, ' ', 2) from fn_test order by kstr"
	qt_sql_substring_index_String_String_Integer_notnull "select substring_index(kstr, ' ', 2) from fn_test_not_nullable order by kstr"
}