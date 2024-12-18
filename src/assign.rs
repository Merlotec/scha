extern crate nalgebra as na;
use std::{collections::HashMap, f64::consts::PI};

use itertools::Itertools;
use nalgebra::{Vector, Vector2};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Circle {
    pub origin: Vector2<f64>,
    pub r: f64,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Intersection {
    Inside(Circle),
    Intersect(Vector2<f64>, Vector2<f64>, bool),
    None,
}

impl Intersection {
    pub fn intersects(&self) -> bool {
        match self {
            Self::None => false,
            _ => true,
        }
    }
}

impl Circle {
    fn new(x: f64, y: f64, r: f64) -> Circle {
        Circle { origin: Vector2::new(x, y), r }
    }

    pub fn area(&self) -> f64 {
        PI * self.r * self.r
    }

    pub fn distance(&self, other: &Circle) -> f64 {
        self.origin.metric_distance(&other.origin)
    }

    // fn intersection_area(&self, other: &Circle) -> f64 {
    //     let d = self.distance(other);

    //     if d >= self.r + other.r {
    //         // No intersection
    //         return 0.0;
    //     }

    //     if d <= (self.r - other.r).abs() {
    //         // One circle is completely inside the other
    //         return PI * self.r.min(other.r).powi(2);
    //     }

    //     let r1_squared = self.r.powi(2);
    //     let r2_squared = other.r.powi(2);

    //     let angle1 = (r1_squared + d.powi(2) - r2_squared) / (2.0 * self.r * d);
    //     let angle2 = (r2_squared + d.powi(2) - r1_squared) / (2.0 * other.r * d);

    //     let theta1 = 2.0 * angle1.acos();
    //     let theta2 = 2.0 * angle2.acos();

    //     let segment1 = 0.5 * r1_squared * (theta1 - theta1.sin());
    //     let segment2 = 0.5 * r2_squared * (theta2 - theta2.sin());

    //     segment1 + segment2
    // }


    fn intersection_area(&self, other: &Circle) -> f64 {
        match self.intersect(other) {
            Intersection::Inside(c) => PI * c.r * c.r,
            Intersection::None => 0.0,
            Intersection::Intersect(a, b, nearside) => {
                let l = a.metric_distance(&b);
                if !nearside {
                    segment_area(self.r, l) + segment_area(other.r, l)
                } else {
                    let (smaller, larger) = if self.r < other.r {
                        (self, other)
                    } else {
                        (other, self)
                    };
                    segment_area(larger.r, l) + PI * smaller.r * smaller.r - segment_area(smaller.r, l)
                }
            },
        }
    }

    pub fn is_inside(&self, other: &Circle) -> bool {
        let d = self.distance(other);
        if d > self.r + other.r {
            false
        } else if d + other.r < self.r {
            true
        } else {
            false
        }
    }

    pub fn intersect(&self, other: &Circle) -> Intersection {
        let d = self.distance(other);
        if d > self.r + other.r {
            Intersection::None
        } else if d + other.r <= self.r {
            Intersection::Inside(*other)
        } else if d + self.r <= other.r {
            Intersection::Inside(*self)
        } else if d == 0.0 {
            // epsilon difference
            Intersection::Inside(*other)
        } else {
            assert_ne!(d, 0.0);
            let (smaller, larger) = if self.r < other.r {
                (self, other)
            } else {
                (other, self)
            };

            let r_sq = larger.r * larger.r;
            let d_sq = d * d;
            let v = (r_sq + d_sq - smaller.r * smaller.r) / (2.0 * d);
            let h_sq = r_sq- v * v;
            let h = h_sq.sqrt();

            let s_sq = r_sq - h_sq;

            let s = s_sq.sqrt();

            let l = smaller.origin - larger.origin;

            let lnorm = l.normalize();

            let mp = larger.origin + (lnorm * s);

            let lperp = Vector2::new(-lnorm.y, lnorm.x);

            let a = mp + lperp * h;
            let b = mp + lperp * (-h);

            let nearside = s > d;

            Intersection::Intersect(a, b, nearside)
        }
    }

    /// Returns the circles that this circle intersects.
    /// Does not compute area of intersection so is fast.
    pub fn intersects_many(&self, others: &[Circle]) -> Vec<Circle> {
        others.iter().filter_map(|x| if self.intersect(x).intersects() { Some(*x) } else { None }).collect()
    }

    /// Calculates the total area that `circle` shares with any other circle in the `others` slice.
    pub fn total_intersection(&self, others: &[Circle]) -> f64 {
        let mut acc: f64 = 0.0;
        for c in 1..=others.len() {
            let polarity: f64 = if c % 2 == 0 {
                -1.0
            } else {
                1.0
            };

            for combs in others.to_vec().into_iter().combinations(c) {
                let mut cs = combs.to_vec();
                cs.push(*self);
                // When polarity is negative we deduct to remove double counting of previous,
                let pl = polarity * Circle::intersect_all(&cs);
                if pl.is_nan() {
                    panic!("Circle intersection NaN!");
                }
                acc += pl;
            }
        }

        acc
    }


    pub fn intersect_all(circles: &[Circle]) -> f64 {
        let mut points: Vec<(Vector2<f64>, usize, usize)> = Vec::new();
        let mut ignores: Vec<usize> = Vec::new();
        let mut nearside: bool = false;

        for combs in circles.iter().enumerate().combinations(2) {
            let (i, c1) = combs[0];
            let (j, c2) = combs[1];
            match c1.intersect(c2) {
                Intersection::Intersect(a, b, ns) => {
                    points.push((a, i, j));
                    points.push((b, i, j));
                    if ns {
                        nearside = true;
                    }
                },
                Intersection::Inside(cx) => {
                    if cx == *c1 {
                        ignores.push(j);
                    } else {
                        ignores.push(i);
                    }
                },
                Intersection::None => {return 0.0}, 
            }
        }

        // only keep intersections in inner most area.
        points.retain(|(p, i, j)| {
            if ignores.contains(i) || ignores.contains(j) {
                return false;
            }

            let mut count = 0;
            for (k, c) in circles.iter().enumerate() {
                if k != *i && k != *j {
                    if p.metric_distance(&c.origin) < c.r {
                        count += 1;
                    }
                }
            }
            count == circles.len() - 2 
        });


        if points.len() > 2 {
                // calculate inner polygon area. 
            let poly_area = polygon_area(points.iter().map(|(p, _, _)| *p).collect::<Vec<Vector2<f64>>>().as_slice());

            // since our shape is convex we can use the 'centre of mass' of the points to determine directions of the normals of the faces, because the centre of mass will lie in the shape for convex shapes.
            let cm = points.iter().fold(Vector2::zeros(), |x, (p, _, _)| x + p) / points.len() as f64;

            let mut acc = 0.0;

            for (c, circle) in circles.iter().enumerate() {
                // Get line segment for circle.
                if !ignores.contains(&c) {
                    let mut p1 = None;
                    let mut p2 = None;
                    for (p, i, j) in points.iter() {
                        if *i == c || *j == c {
                            if p1.is_none() {
                                p1 = Some(*p);
                            } else {
                                p2 = Some(*p);
                            }
                        }
                    }

                    if let (Some(p1), Some(p2)) = (p1, p2) {
                        let v = p2 - p1;
                        let l = v.norm();
                        let vn = v.normalize();
                        let n1 = Vector2::new(-vn.y, vn.x);
    
                        let vcm = cm - p1;
    
                        let segnorm = if vcm.dot(&n1) < 0.0 {
                            n1
                        } else {
                            -n1
                        };
    
                        let cv = circle.origin - p1;
                        if cv.dot(&segnorm) <= 0.0 {
                            // Usual situation - smaller segment of the circle to be added.
                            acc += segment_area(circle.r, l);
                        } else {
                            // Unusual situation - larger segment of the circle to be added.
                            acc += PI * circle.r * circle.r - segment_area(circle.r, l); // Add half of the circle
                        }
                    } 
                }
            }
            poly_area + acc
        } else if points.len() == 2 {
            let mut c1 = None;
            let mut c2 = None;

            for (i, c) in circles.iter().enumerate(){
                if !ignores.contains(&i) {
                    if c1.is_none() {
                        c1 = Some(*c);
                    } else {
                        c2 = Some(*c);
                    }
                }
            }

            let c1 = c1.unwrap();
            let c2 = c2.unwrap();
            c1.intersection_area(&c2)
        } else {
            let remaining: Vec<usize> = (0..circles.len()).filter_map(|x| if !ignores.contains(&x) { Some(x) } else { None }).collect_vec();
            if remaining.len() == 1 {
                circles[remaining[0]].area()
            } else {
                0.0 // No intersections
            }
        }
    }

    pub fn group<C: Into<Circle> + Clone>(circles: &[C]) -> Vec<Vec<usize>> {
        let mut groups: Vec<Vec<usize>> = Vec::new();
        for combs in circles.into_iter().enumerate().combinations(2) {
            let (i, a): (usize, Circle) = (combs[0].0, combs[0].1.clone().into());
            let (j, b): (usize, Circle) = (combs[1].0, combs[1].1.clone().into());
    
            if a.intersect(&b) != Intersection::None {
                let mut pr_app: Option<Vec<usize>> = None;
                let mut principle: Option<usize> = None;
        
                let mut idx = 0;
                groups.retain_mut(|gr| {
                    let g = idx;
                    idx += 1;
                    if gr.contains(&i) {
                        if principle.is_some() {
                            pr_app = Some(gr.clone());
                            return false;
                        } else {
                            if !gr.contains(&j) {
                                gr.push(j);
                            }
                            principle = Some(g);
                            return true;
                        }
                    }
                    if gr.contains(&j) {
                        if principle.is_some() {
                            pr_app = Some(gr.clone());
                            return false;
                        } else {
                            if !gr.contains(&i) {
                                gr.push(i);
                            }
                            principle = Some(g);
                            return true;
                        }
                    }
                    true
                });
        
                if let (Some(g), Some(pr_app)) = (principle, pr_app) {
                    for k in pr_app {
                        if !groups[g].contains(&k) {
                            groups[g].push(k);
                        }
                    }
                } else if principle.is_none() {
                    groups.push(vec![i, j]);
                }
            }
        }
        groups
    }

}


fn segment_area(r: f64, l: f64) -> f64 {
    if l > 2.0 * r {
        panic!("Chord length cannot be greater than the diameter of the circle");
    }

    //let h: f64 = r - ((r * r - (l * l) / 4.0).sqrt());
    let theta = 2.0 * ((l / (2.0 * r)).asin());
    let area = 0.5 * r * r * (theta - theta.sin());

    area
}

fn polygon_area(vertices: &[Vector2<f64>]) -> f64 {
    let n = vertices.len();
    if n < 3 {
        return 0.0; // Not a polygon
    }

    let mut area = 0.0;
    for i in 0..n {
        let j = (i + 1) % n;
        area += vertices[i].x * vertices[j].y;
        area -= vertices[j].x * vertices[i].y;
    }

    (area / 2.0).abs()
}

#[test]
pub fn circle_test() {
    let a = Circle::intersect_all(&[
        Circle::new(0.48, 0.99, 0.69),
        Circle::new(0.48, 0.69, 0.39),
        Circle::new(0.99, 0.64, 0.71),
    ]);

}

#[test]
pub fn segment_check() {
    let a = segment_area(20.0, 24.0);
    assert!(a > 65.35 && a < 65.45);
}

#[test]
pub fn circle_check() {
    let c1 = Circle::new(0.0, 0.0, 1.0);
    let c2 = Circle::new(0.5, 0.0, 0.7);
    let c3 = Circle::new(0.0, -0.5, 0.8);
    let c4 = Circle::new(0.0, 0.5, 0.8);
    let a = Circle::intersect_all(&[c1, c2, c3, c4]);

    assert!(a > 3.666 && a < 0.3668);
}

#[test]
pub fn super_circle() {
    let a = Circle::intersect_all(&[
        Circle::new(0.0, 0.0, 1.0),
        Circle::new(0.5, 0.0, 0.7),
        Circle::new(0.0, -0.5, 0.8),
        Circle::new(0.0, 0.5, 0.8),
    ]);
    let b = Circle::intersect_all(&[
        Circle::new(0.0, 0.0, 1.0),
        Circle::new(0.5, 0.0, 0.7),
        Circle::new(0.0, -0.5, 0.8),
        Circle::new(0.0, 0.5, 0.8),
        Circle::new(0.0, 0.5, 0.9),
    ]);
    assert_eq!(a, b);
}

pub struct CircleRecord {
    pub area: f64,
    pub circle: Circle,
    pub absolute_weight: f64, 
}

// pub fn scale_group(circle: &[Circle]) -> Vec<Circle> {
    
// }

// pub fn scale_all(circles: &[Circle]) -> Vec<CircleRecord> {
//     //group
//     let groups = Circle::group(circles);

//     // grow each group
//     for group in groups {
//         for circle in circles 
//     }
// }

// pub fn intersect_exclusive(circles: &[Circle], intersections: &[usize]) -> f64 {
//     // start with base intersection:
//     let base: Vec<Circle> = circles.iter().enumerate().filter_map(|(i, x)| {
//         if intersections.contains(&i) {
//             Some(*x)
//         } else {
//             None
//         }
//     }).collect_vec();
//
//     let others: Vec<usize> = (0..circles.len()).filter_map(|x| {
//         if intersections.contains(&x) {
//             None
//         } else {
//             Some(x)
//         }
//     }).collect();
//
//     let mut acc: f64 = Circle::intersect_all(&base);
//     for c in 1..(others.len() + 1) {
//         let polarity: f64 = if c % 2 == 0 {
//             1.0
//         } else {
//             -1.0
//         };
//
//         for combs in others.iter().combinations(c) {
//             let cs: Vec<Circle> = circles.iter().enumerate().filter_map(|(i, x)| {
//                 if combs.contains(&&i) || intersections.contains(&i) {
//                     Some(*x)
//                 } else {
//                     None
//                 }
//             }).collect_vec();
//
//             acc += polarity * Circle::intersect_all(&cs);
//         }
//     }
//
//     acc
// }

// Outputs the new radius of the target circle to take up the specified area that does not intersect with any other circle.
#[derive(Debug, Copy, Clone)]
pub struct RadialArea {
    pub origin: Vector2<f64>,
    pub area: f64,
}

pub fn scale_to_exclusive_area(circles: &[Circle], radial: &RadialArea, mut delta: f64, epsilon: f64, max_iter: usize) -> Option<Circle> {
    let mut r = (radial.area / PI).sqrt();
    let mut a_prev = None;
    for _ in 0..max_iter {
        let circle = Circle { r, origin: radial.origin };
        let ints = circle.intersects_many(circles);
        let intersection = circle.total_intersection(&ints);
        assert!(intersection >= 0.0);
        let a_total = circle.area() - intersection;

        if (a_total - radial.area).abs() < epsilon {
            return Some(circle);
        }

        if a_total < radial.area {
            r += delta;
            if a_prev > Some(radial.area) {
                delta *= 0.5;
            }
        } else if a_total > radial.area {
            r -= delta;
            if a_prev < Some(radial.area) {
                delta *= 0.5;
            }
        }

        a_prev = Some(a_total);
    }

    None
}

pub fn scale_all(radials: &[RadialArea], delta: f64, epsilon: f64, max_iter: usize) -> Option<Vec<Circle>> {
    let mut circles = Vec::with_capacity(radials.len());

    for radial in radials {
        circles.push(scale_to_exclusive_area(&circles, radial, delta, epsilon, max_iter)?)
    }

    Some(circles)
}

#[test]
fn test_groups() {
    let gs = Circle::group(&[
        Circle::new(0.0, 0.0, 1.0),
        Circle::new(0.5, 0.0, 0.7),
        Circle::new(0.0, -0.5, 0.8),
        Circle::new(0.0, 0.5, 0.8),
        Circle::new(0.0, 0.5, 0.9),
        Circle::new(20.0, 0.0, 1.0),
        Circle::new(20.5, 0.0, 0.7),
        Circle::new(20.0, -0.5, 0.8),
        Circle::new(20.0, 0.5, 0.8),
        Circle::new(20.0, 0.5, 0.9),
        Circle::new(20.0, 0.5, 50.0),
    ]);

    println!("groups: {:?}", gs);
}

#[test]
fn test_intersect_exclusive() {
    let gs = &[
        Circle::new(0.0, 0.0, 1.0),
        Circle::new(0.5, 0.0, 0.7),
        Circle::new(0.0, -0.5, 0.8),
        Circle::new(0.0, 0.5, 0.8),
        Circle::new(0.0, 0.5, 0.9),
        Circle::new(20.0, 0.0, 1.0),
        Circle::new(20.5, 0.0, 0.7),
        Circle::new(20.0, -0.5, 0.8),
        Circle::new(20.0, 0.5, 0.8),
        Circle::new(20.0, 0.5, 0.9),
    ];

    let a = Circle::new(0.0, 0.0, 5.0).total_intersection(gs);

    println!("intersect_exclusive: {:?}", a);
}


#[test]
fn test_area_scale() {
    let gs = &[
        Circle::new(0.0, 0.0, 1.0),
        Circle::new(0.5, 0.0, 0.7),
        Circle::new(0.0, -0.5, 0.8),
        Circle::new(0.0, 0.5, 0.8),
        Circle::new(0.0, 0.5, 0.9),
        Circle::new(20.0, 0.0, 1.0),
        Circle::new(20.5, 0.0, 0.7),
        Circle::new(20.0, -0.5, 0.8),
        Circle::new(20.0, 0.5, 0.8),
        Circle::new(20.0, 0.5, 0.9),
    ];

    let c = scale_to_exclusive_area(gs, &RadialArea { origin: Vector2::new(0.0, 0.0), area: PI }, 1.0, 0.001, 200);

    println!("intersect_exclusive: {}", c.unwrap().r);
}

#[test]
fn test_scale_all() {
    let gs = &[
        RadialArea { origin: Vector2::new(0.0, 0.0), area: PI },
        RadialArea { origin: Vector2::new(0.2, 0.7), area: PI },
        RadialArea { origin: Vector2::new(1.2, -0.3), area: PI },
        RadialArea { origin: Vector2::new(-0.4, 0.5), area: PI },
    ];

    let c = scale_all(gs, 1.0, 0.001, 200);

    println!("scale_all: {:?}", c);
}