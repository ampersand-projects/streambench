set grid
set term pdf font 'Verdana,18'
set output "e2e.pdf"
set datafile separator ","
set key right top horizontal
set xtics rotate by 45 right
set style data histogram
set style fill solid border
set ylabel "million events/sec"
set yrange [0:300]

#set logscale y 10 

set style histogram clustered
plot newhistogram, 'trill_bench/trill_real.csv' u 2:xticlabels(1) t 'Trill' lc 7 fs pattern 2,\
                   'tilt_bench/tilt_real.csv' u 2:xticlabels(1) t 'TiLT' lc 6 fs pattern 3
