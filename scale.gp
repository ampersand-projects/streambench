set grid
set term pdf font 'Verdana,18'
ofile = "ysb.pdf"
set key top outside horizontal
set output ofile
set datafile separator ","
set ylabel "million events/sec"
set xlabel "Number of threads"

set logscale x 2

plot "lightsaber2_bench/lightsaber_yahoo.csv" u 1:2 w lp t 'LightSaber' lw 1 pt 1 ps .5 lc 1,\
     "grizzly_bench/grizzly_yahoo.csv" u 1:2 w lp t 'Grizzly' lw 1 pt 2 ps .5 lc 2,\
     "streambox_bench/streambox_yahoo.csv" u 1:2 w lp t 'StreamBox' lw 1 pt 3 ps .5 lc 8,\
     "trill_bench/trill_yahoo.csv" u 1:2 w lp t 'Trill' lw 1 pt 7 ps .5 lc 7,\
     "tilt_bench/tilt_yahoo.csv" u 1:2 w lp t 'TiLT' lw 1 pt 5 ps .5 lc 6

