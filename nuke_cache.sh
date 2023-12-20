 function nuke_if_too_big() {
   path=$1
   limit_mb=$2
   size_mb=$(du -m -d0 "${path}" | cut -f 1)
   if (( size_mb > limit_mb )); then
     echo "${path} is too large (${size_mb}mb), nuking it."
     rm -rf "${path}"
   fi
 }

nuke_if_too_big ~/.cache/pants/setup 512
nuke_if_too_big ~/.cache/pants/named_caches 1024