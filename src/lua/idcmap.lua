
primary = {
   tc = {"$master"},
   jx = {"$master"},
   nj02 = {"$master"},
   nj03 = {"$master"},
   hz01 = {"$master"},
}

primary_preferred = {
   tc = {"$master","tc","jx",{"nj","nj03","hz"}},
   jx = {"$master","jx","tc",{"nj","nj03","hz"}},
   nj = {"$master","nj","nj03","hz",{"tc","jx"}},
   nj03 = {"$master","nj03","nj","hz",{"tc","jx"}},
   hz = {"$master","hz",{"nj","nj03"},{"tc","jx"}}
}

nearest = {
   tc = {"tc","jx",{"nj","nj03","hz"}},
   jx = {"jx","tc",{"nj","nj03","hz"}},
   nj = {"nj","nj03","hz",{"tc","jx"}},
   nj03 = {"nj03","nj","hz",{"tc","jx"}},
   hz = {"hz",{"nj","nj03"},{"tc","jx"}}
}

return nearest
