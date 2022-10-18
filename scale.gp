set grid
set term pdf font 'Verdana,18'
ofile = "ysb_tp.pdf"
set key top outside horizontal
set output ofile
set datafile separator ","
set ylabel "million events/sec"
set xlabel "Number of threads"

set logscale x 2

plot "lightsaber_ysb_tp.csv" u 1:2 w lp t 'LightSaber' lw 1 pt 1 ps .5 lc 1,\
	 "grizzly_ysb_tp.csv" u 1:2 w lp t 'Grizzly' lw 1 pt 2 ps .5 lc 2,\
	 "streambox_ysb_tp.csv" u 1:2 w lp t 'StreamBox' lw 1 pt 3 ps .5 lc 8,\
	 "trill_ysb_tp.csv" u 1:2 w lp t 'Trill' lw 1 pt 7 ps .5 lc 7,\
	 "tilt_ysb_tp.csv" u 1:2 w lp t 'TiLT' lw 1 pt 5 ps .5 lc 6

