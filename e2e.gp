set grid
set term png font 'Verdana,18'
set output "e2e.png"
set datafile separator ","
set key left top horizontal
set xtics rotate by 45 right
set style data histogram
set style fill solid border
set ylabel "million events/sec"
set yrange [0:400]

set style histogram clustered
plot newhistogram, 'data.csv' u 2:xticlabels(1) t 'Trill' lc 7 fs pattern 2,\
                   'data.csv' u 0:2:4 with labels offset -1.0,0.4 font ",10" rotate by 0 notitle,\
                   'data.csv' u 3:xticlabels(1) t 'TiLT' lc 6 fs pattern 3,\
                   'data.csv' u 0:3:5 with labels offset 0.5,0.4 font ",10" rotate by 0 notitle,\
